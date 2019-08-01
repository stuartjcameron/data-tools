# -*- coding: utf-8 -*-
"""
Access the UNESCO Institute of Statistics SDMX API, download data
in a convenient format, and find information on indicators that are available
in the database.

TODO: write a helper function for filtering dataframes the same way that
the API query can be filtered, e.g. query_df(df, ref_area=["BD", "TZ"], sex="F")

@author: https://github.com/stuartjcameron
"""
import csv
import os
import logging as lg
from itertools import permutations, chain

import string_utils
import sdmx_api
from sdmx_response import SdmxJsonResponse, SdmxCsvResponse, METADATA, cached_property

try:
    from hdx.location.country import Country
except ImportError:
    pass

UIS_BASE = "https://api.uis.unesco.org/sdmx/data/UNESCO,EDU_NON_FINANCE,3.0"
UIS_DIMENSIONS = [
           "STAT_UNIT",
           "UNIT_MEASURE",
           "EDU_LEVEL",
           "EDU_CAT",
           "SEX",
           "AGE",
           "GRADE",
           "SECTOR_EDU",
           "EDU_ATTAIN",
           "WEALTH_QUINTILE",
           "LOCATION",
           "EDU_TYPE",
           "EDU_FIELD",
           "SUBJECT",
           "INFRASTR",
           "SE_BKGRD",
           "TEACH_EXPERIENCE",
           "CONTRACT_TYPE",
           "COUNTRY_ORIGIN",
           "REGION_DEST",
           "IMM_STATUS",
           "REF_AREA"
            ]
lg.info("CWD={}; file path={}".format(os.getcwd(), os.path.dirname(__file__)))
INDICATOR_DATA = os.path.join(os.path.dirname(__file__), "input-data/combined indicators.csv")
uis_filter = sdmx_api.Filter(UIS_DIMENSIONS)




def get_iso2(s, use_live=False):
    if Country:
        iso3, fuzzy = Country.get_iso3_country_code_fuzzy(s, use_live)
        if iso3:
            return Country.get_country_info_from_iso3(iso3)["#country+code+v_iso2"]
    else:
        return s.upper()


def get_indicator_keys():
    """ 
    Returns a list of dicts containing information on all of the UIS
    indicators, from information in a CSV file.
    """
    key_types = ["key", "short_key", "Indicator ID", "Indicator Label - EN"]
    keys = {k: [] for k in key_types}
    with open(INDICATOR_DATA, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            for t in key_types:
                keys[t].append(row[t])
    keys["id"] = keys["Indicator ID"]
    keys["id_lower"] = [k.lower() for k in keys["id"]]
    keys["key_lower"] = [k.lower() for k in keys["key"]]
    keys["label"] = keys["Indicator Label - EN"]
    return keys


def get_indicator_df(columns=None):
    """
    Returns a Pandas dataframe based on the CSV file with ID etc. added for 
    easy lookup, indexed by SDMX key
    """
    import pandas as pd
    if columns is not None:
        columns = set(list(columns) + ["Indicator ID", "key"])
    df = pd.read_csv(INDICATOR_DATA, usecols=columns)
    df["id_lower"] = df["Indicator ID"].str.lower()
    df["key_lower"] = df["key"].str.lower()    
    df.set_index("key", inplace=True)
    #if columns is not None:
    #    df = df[columns]
    return df


def get_country_df(columns=None):
    """
    Convert the HDX country database into a dataframe, indexed by ISO2
    If columns is not None, select the specified list of columns only.
    """
    import pandas as pd
    country_data = Country.countriesdata(use_live=False)["countries"]
    df = pd.DataFrame.from_records(list(country_data.values()))
    df.set_index("#country+code+v_iso2", inplace=True)
    if columns is not None:
        df = df[list(columns)]
    return df


class allbut(object):
    """
    Set-like class to represent all but the listed members
    e.g. [1,2,3,4] - allbut(1, 2) will return [3, 4]
    """    
    def __init__(self, *args):
        self.members = set(args)
    
    def __rsub__(self, other):
        return list(set(other) - self.members)
        
def get_filters(df, filter_dict):
    """ 
    Generate filter columns based on a dictionary of column names
    and values
    Values can be individual strings/values, lists, or instances of allbut
    """    
    def transform1(heading):
        if isinstance(heading, str):
            return heading.lower()
        else:
            return heading
        
    def transform2(heading):
        if isinstance(heading, str):
            return string_utils.clean_label(heading)
        else:
            return heading
    
    funcs = [transform1, transform2]
    altered_columns = [(f, {f(h): h for h in df.columns}) for f in funcs]
    for k, v in filter_dict.items():
        if k not in df.columns:
            for transform, headings in altered_columns:
                if k in headings:
                    k = headings[k]
                    break
            else:
                raise KeyError("Keyword {} not found in dataset".format(k))
        if isinstance(v, list):
            yield df[k].isin(v)
        elif isinstance(v, allbut):
            yield ~df[k].isin(v.members)
        else:
            yield df[k] == v   
    
def filter_df(df, **kwargs):
    """
    Convenience filter for dataframes
    Allows slightly fuzzy matching to column headings e.g. 'stat_unit'
    will match 'STAT_UNIT' or 'Stat. Unit' if an exact match isn't found
    """
    import pandas as pd
    return df[pd.DataFrame(get_filters(df, kwargs)).all()]

def drop_redundant_cols(df):
    """
    Remove all columns which are all the same
    Returns the values of those columns and the remaining df
    Taken from https://stackoverflow.com/a/39658662/567595
    """
    import pandas as pd
    nunique = df.apply(pd.Series.nunique)
    cols_to_drop = nunique[nunique == 1].index
    return df[cols_to_drop].iloc[0], df.drop(cols_to_drop, axis=1)            
    

class Api(sdmx_api.Api):
    """ 
    Access data from the UNESCO Institute of Statistics and return it in a 
    convenient format.
    
    This is a tailored version of the more generic sdmx_api.
    """

    def __init__(self, subscription_key):
        #self.super = super(Api, self)
        self.super = super()
        self.super.__init__(base=UIS_BASE,
                                subscription_key=subscription_key,
                                dimensions=UIS_DIMENSIONS)
        self.process_response = {"sdmx-json": JsonResponse,
                                 "csv": CsvResponse}
        
        
    def query(self, ind=None, country=None, start=None, end=None,
                    by=None, disag_only=False, 
                    use_live_country_info=False, 
                    data_format=None, **kwargs):
        if ind:
            query = Indicator.query(lookup=ind, by=by, disag_only=disag_only)
            looked_up = sdmx_api.combine_queries(*[ind.spec for ind in query])
            if not looked_up:
                raise KeyError("Indicator {} not found".format(ind))
            query = {**kwargs, **looked_up}
        else:
            query = kwargs
        if country:
            if type(country) is not list:
                country = [country]
            query["ref_area"] = [get_iso2(s, use_live=use_live_country_info) 
                                for s in country]
        if start:
            query["start_period"] = start
        if end:
            query["end_period"] = end
        return self.super.query(data_format=data_format, 
                                dimension_at_observation="AllDimensions", 
                                **query)
        #response.set_structure(ref_area="REF_AREA", time_period="TIME_PERIOD")
        #return response
    

class CsvResponse(SdmxCsvResponse):
    def __init__(self, response):
        self.super = super()
        self.super.__init__(response)
        self.set_structure(ref_area="REF_AREA", time_period="TIME_PERIOD")
    
    @cached_property
    def dataframe(self):
        #TODO: consider adding indicator information to the dataframe too
        df = self.super.dataframe
        df = add_country_info_to_df(df, {
            "#country+name+preferred": "Country name",
            "#country+alt+i_en+name+v_unterm": "UN country name",
            "#region+name+preferred+sub": "Region"                
            })
        return df
        


class JsonResponse(SdmxJsonResponse):
    """
    Extends the SdmxJsonResponse class to add some specific processing for the 
    UIS API response.
    """
    
    def __init__(self, response):
        #self.super = super(Response, self)
        self.super = super()
        self.super.__init__(response)
        self.set_structure(ref_area="REF_AREA", time_period="TIME_PERIOD")
        
    def get_nested(self, metadata=METADATA.ALL, use_uis_ids=True):
        """ Return the data in a convenient nested format
        {indicator: {country: {year: value}}..., metadata: {}}
        
        By default all metadata will be included.
        metadata=None will only return the data.
        
        If use_uis_ids=True then UIS indicator IDs will be returned
        Otherwise indicators will be identified using SDMX keys.
        """
        def adjust_key(k):
            if k != "metadata":
                try:
                    return Indicator(key=k).id
                except:
                    pass
            return k
        
        r = self.super.get_nested(metadata)
        if use_uis_ids:
            r = {adjust_key(k): v for k, v in r.items()}
            if metadata:
                m = r["metadata"]
                if "indicators" in m:
                    m["indicators"] = {adjust_key(k): v
                         for k, v in m["indicators"].items()}
        return r
    
    @cached_property
    def dataframe(self):
        df = self.super.dataframe
        df = add_country_info_to_df(df, {
            "#country+name+preferred": "Country name",
            "#country+alt+i_en+name+v_unterm": "UN country name",
            "#region+name+preferred+sub": "Region"                
            })
        df = add_indicator_info_to_df(df, [
                'Indicator Label - EN', 
                'Indicator Section', 
                'Table query', 
                'Theme'
                ])
        df["Year"] = df["TIME_PERIOD"].astype("int")
        #df.rename(columns=header_case, inplace=True)
        return df
   

        
def add_country_info_to_df(df, columns=None):
    return merge_df(df, get_country_df, on="REF_AREA", columns=columns)
    

def add_indicator_info_to_df(df, columns=None):
    return merge_df(df, get_indicator_df, on="Indicator key", columns=columns)


def merge_df(left, lookup_func, on, columns=None):
    """ Merge a dataframe with selected columns from a second dataset
    accessed through a lookup function. 
    
    If columns is None - adds all columns from the second dataset
                  a list - adds listed columns from the second dataset
                  a dict - adds listed columns in the second dataset
                          and renames them when adding to the first dataset
    """
    try:
        lookup_columns = columns.keys()
        rename = True
    except AttributeError:
        lookup_columns = columns
        rename = False
    right = lookup_func(columns=lookup_columns)
    df = left.merge(right=right, left_on=on, right_index=True)
    if rename:
        df.rename(columns=columns, inplace=True)
    return df


class Indicator(object):
    """ 
    Get information on indicators in the UNESCO Institute of Statistics 
    database.
    
    Based on a spreadsheet accessed 2019-05 at
    http://uis.unesco.org/sites/default/files/documents/uis-data-dictionary-education-statistics.xlsx
    
    combined with information obtained directly from the UIS data API.
    
    """
    keys = get_indicator_keys()
    key_types = ["key", "short_key", "id"] 
    SUB = object()
    ALL = object()    
    
    def __init__(self, id=None, key=None, short_key=None, index=None):
        if index is None:
            index = self._get_index(key, id, short_key)
        self.key, self.short_key, self.id = (self.keys[t][index]
            for t in self.key_types)
        self.label = self.keys["label"][index]
        self.spec = uis_filter.key_to_dict(self.key)
    
    
    @classmethod
    def _get_index(cls, key=None, id=None, short_key=None):
        if key:
            return cls.keys["key_lower"].index(key.lower())
        elif id:
            return cls.keys["id_lower"].index(id.lower())
        elif short_key:
            return cls.keys["short_key"].index(short_key.lower())
        
        
    @classmethod
    def query(cls, lookup, by=None, disag_only=True):
        """
        Flexible lookup method for use in API queries.
        
        lookup can be a string containing a key, id or short_key, or a list of them
        by can be cls.SUB, cls.ALL, a dimension string, or a list of dimension strings\
        
        Returns a list of indicators.
        """
        if type(lookup) is list:
            result = [cls.lookup(s) for s in lookup]
        else:
            result = [cls.lookup(lookup)]
        return sorted(cls.disaggregate_inds(result, by, disag_only))
        
    @classmethod
    def lookup(cls, s):
        """ Lookup by key, id, or short key """
        try:
            s = s.lower()
        except AttributeError:
            return s   # Not a string so just try returning it directly
        if s in cls.keys["key_lower"]:
            return cls(key=s)
        if s in cls.keys["id_lower"]:
            return cls(id=s)
        if s in cls.keys["short_key"]:
            return cls(short_key=s)
        raise KeyError("Key {} not found".format(s))
    
    @classmethod
    def disaggregate_inds(cls, result, by, disag_only=True):
        if by is None:
            return result
        if type(by) is str:
            by = [by]
        if type(by) is list:
            lg.info("Disaggregating by dimensions {}".format(by))
            disaggregated, matched = cls.match_similar(result, ignore=by)
            if disag_only:
                r = matched | disaggregated # only return the result indicators if they could be disaggregated
            else:
                r = set(result) | disaggregated # return all of the result indicators
        else:
            lg.info("Disaggregating {}, by {}".format(result, by))
            r = set(result) | set(chain.from_iterable(ind.disaggregate(by) for ind in result))
        if r:
            return list(r)
        else:
            raise ValueError("No indicators found that can be "
                             "disaggregated by {}".format("/".join(by)))    
        
    @classmethod
    def fuzzy_lookup(cls, s, by=None, shortest=True, allow_multiple=True,
                     disag_only=True):
        """
        Finds one or more indicators matching the given string query.
        Looks first for an exact match by ID, key or short key.
        
        If that fails it looks for keys, short keys or IDs that contain
        all parts (separated by spaces) of the string.
        
        Returns a single indicator where possible, or a list where there
        are multiple matches if allow_multiple=True.
        
        by can be None, a list of dimensions for disaggregation, or 
        - Indicator.SUB to get all 'children' (main indicator + one level of 
        disaggregation)
        - Indicator.ALL to get all disaggregations (main indicator and all
         possible levels of disaggregation)
        
        If disag_only is True and by is a list of dimensions, then
        only indicators that can be disaggregated by one or more of those 
        dimensions will be returned.
        
        TODO: consider an option to exclude the top level indicator
        when disaggregating
        TODO: refactor to avoid repetition (e.g. match_similar)
        """
        if type(by) not in [str, list] and by not in [cls.SUB, cls.ALL, None]:
            raise TypeError("by must be a string, list, SUB, ALL or None")
        
        def finalize(result):
            return sorted(cls.disaggregate_inds(result, by, disag_only))

        def interpret_indices(indices):
            if indices:
                matches = [cls(index=i) for i in indices]
                #if shortest:
                #    min_length = min((m.parts(), len(m)) for m in matches)
                #    matches = [m for m in matches 
                #               if (m.parts(), len(m)) == min_length]
                # Note: changed this algorithm. Looking at the length
                # of the short key is not a good way to select in general!
                # e.g. "ofst" -> FOFSTP.1 but not OFST.1.CP
                
                # If we are only looking for disaggregatable indicators, then
                # filter for these first.
                if type(by) is list and disag_only:
                    _, matches = cls.match_similar(matches, ignore=by)
                    if not matches:
                        return None
                if shortest:
                    min_length = min(m.parts() for m in matches)
                    matches = [m for m in matches if m.parts() == min_length]
                if len(matches) == 1 or allow_multiple:
                    return matches
            return None
        
        # First look for an exact match
        try:
            r = [cls.lookup(s)]
        except KeyError:
            pass
        else:
            return finalize(r)
        
        # Then look for a strict, then a loose substring match
        substrings = tuple(s.split(" "))
        for match in [strict_key_match, loose_key_match]:
            lg.info("Attempting to match using %s", match)
            indices = set()
            for lookup in ["short_key", "key_lower", "id_lower"]:
                if lookup == "short_key":
                    sep = "-"
                else:
                    sep = "."            
                for i, key in enumerate(cls.keys[lookup]):
                    key_parts = key.split(sep)
                    if match(substrings, key_parts):
                        indices.add(i)
            r = interpret_indices(indices)
            if r:
                return finalize(r)
                
        # Then look for a label match
        lg.info("Trying a label match")
        clean_labels = [string_utils.clean_label(k) for k in cls.keys["label"]]
        indices = best_matches(s, clean_labels)
        r = interpret_indices(indices)
        if r:
            return finalize(r)
        
        # Everything failed
        raise ValueError("No indicators found that match '{}'".format(s))
    
    def disaggregate(self, by):
        cls = self.__class__
        if by == self.SUB:
            r = [cls(short_key=ind) for ind in self.get_children()]
            lg.info("disaggregating to {}".format(r))
            return r
        elif by == self.ALL:
            return [cls(short_key=ind) for ind in self.get_descendents()]
        else:
            return self.similar(by)
        
    def get_relations(self, relation, reverse=False):
        """ Get all indicator short keys for which the relation function is True """
        return list(get_relations(short_key=self.short_key,
                                  all_short_keys = self.keys["short_key"],
                                  relation=relation,
                                  reverse=reverse))
    
    def get_parents(self):
        """ Yields all parents of the current indicator, i.e. indicators
        that can be disaggregated to get the current one. """
        return self.get_relations(is_parent)
    
    def get_children(self):
        return self.get_relations(is_parent, reverse=True)
                    
    def get_ancestors(self):
        return self.get_relations(is_ancestor)
        
    def get_descendents(self):
        return self.get_relations(is_ancestor, reverse=True)
    
    def get_root(self):
        """ Return the first ancestor indicator that has no ancestors """
        return get_root(self.short_key, self.keys["short_key"])
    
    @classmethod
    def get_roots(cls):
        """ Yield all the root indicator short keys """
        for short_key in cls.keys["short_key"]:
            yield short_key, get_root(short_key, cls.keys["short_key"])
            
    @classmethod
    def get_root_set(cls):
        return set(root for _, root in cls.get_roots())
    
    @classmethod
    def match_spec(cls, spec):
        """ List all indicators in the dictionary that fit a given specification
        TODO: This is slow for some reason
        Simply converting all keys to dicts takes some time.
        Consider making a version that compares lists instead of dictionary. """
        def gen():
            for key in cls.keys["key"]:
                ind = uis_filter.key_to_dict(key)
                if specs_match(spec, ind):
                    yield cls(key=key)
                    
        return list(gen())
              
    
    @classmethod
    def match(cls, **kwargs):
        return cls.match_spec({k.upper(): v for k, v in kwargs.items()})
            
        
    def __hash__(self):
        return hash(self.key)
        
    def __eq__(self, other):
        return self.key == other.key
        
    def parts(self):
        """ Number of parts of the short key """
        return len(self.short_key.split("-"))
    
    def similar(self, ignore):
        """ 
        Return list of indicators that resemble the current indicator 
        except for the given dimensions.
        
        Note: this is a bit slow, so I have implemented a faster class-level
        method match_similar which can do the same for several indicators.
        """
        ignore = [k.upper() for k in ignore]
        if not all(uis_filter.is_dim(k) for k in ignore):
            raise ValueError("Dimension does not exist")
        spec = {k: v for k, v in self.spec.items() if not k in ignore}
        return [ind for ind in self.match_spec(spec) if not ind == self]
        
            
    @classmethod
    def match_similar(cls, inds, ignore):
        """
        Return a 2-ple of sets of (i) indicators in the database that are 
        similar to one or more of those in inds and (ii) indicators in inds
        that are similar to one or more of those in the database.
        
        'Similar' means they are the same in all dimensions except those
        dimensions listed in ignore.
        """
        ignore = [k.upper() for k in ignore]
        try:
            ignore_indices = [uis_filter.dims().index(dim) for dim in ignore]
        except ValueError:
            raise ValueError("Dimension does not exist")
        
        def reduce_key(key):
            """
            Converts an indicator key into a tuple of dimension values
            removing the ignored dimensions
            """
            return tuple(dim for i, dim in enumerate(key.split("."))
                    if not i in ignore_indices)
        
        all_reduced_keys = [(k, reduce_key(k)) for k in cls.keys["key"]]
        similar_keys = set()
        matched_needles = set()
        for ind in inds:
            reduced = reduce_key(ind.key)
            similar_to_ind = {k for k, r in all_reduced_keys
                                if r == reduced and k != ind.key}
            if similar_to_ind:
                similar_keys |= similar_to_ind
                matched_needles.add(ind)
        similar_inds = {cls(key=k) for k in similar_keys}
        return similar_inds, matched_needles
        
        
    #def __len__(self):
    #    return len(self.short_key)    
    def comparison_key(self):
        return self.short_key.split("-")
    
    def __lt__(self, other):
        return self.comparison_key() < other.comparison_key()
        
    def matches_spec(self, spec):
        """ Whether or not a given spec matches the indicator """
        return specs_match(spec, self.spec)
    
    
    def __repr__(self):
        return 'uis.Indicator("{}")'.format(self.id)




def max_indices(it):
    """ Return the max and the indices of the maxima in an iterable.
    Similar to L = list(it); return [i for i, s in enumerate(L) if s == max(L)]"""
    indices = None
    highest = None
    for i, n in enumerate(it):
        if i == 0 or n > highest:
            indices = [i]
            highest = n
        elif n == highest:
            indices.append(i)
    return highest, indices


def best_matches(a, L):
    """ Find most similar string in L to a 
    Use the following methods, only proceeding to the next method if there is 
    a tie or nothing found:
    1. check for a in b
    2. check for most words from a found in b
    3. check for ordering of words
    """
    # 1. check for a in b
    lg.info("Matching {}".format(a))
    matches = [i for i, b in enumerate(L) if a in b]
    lg.info("Full matches: {}".format(matches))
    if matches:
        return matches # full match, so won't be able to refine this any further

    # 2. check for max words from a in b
    a = a.split(" ")
    L = [s.split(" ") for s in L]
    highest, matches = max_indices(count_matching_words(a, b) for b in L)
    lg.info("Matches by max words: max {} matches {}".format(highest, matches[:50]))
    if highest == 0:
        return []       # Nothing contains any matching words
    if len(matches) == 1: 
        return matches   # no match or one unique match    
    
    # Multiple matches so check similarity
    highest, doubly_matched = max_indices(matchingness(a, L[i]) for i in matches)
    matches = [matches[i] for i in doubly_matched]
    lg.info("Matches by similarity: max {} matches {}".format(highest, matches[:50]))
    return matches


def count_matching_words(a, b):
    return sum((word in b) for word in a)

    
def matchingness(a, b):
    """ Return a tuple of the longest matched sequences from list or string a
    found in list or string b, providing a measure of their similarity
    
    e.g. (1, ) -- whole of a is found in b 
    () -- nothing from a is found in b
    (.75, .25) -- a sequence three-quarters the length of a was found in b
        and then a sequence one-quarter the length of a was found in the 
        remainder of b
        
    The sum of the tuple is the proportion of a found in b. Appending the sum
    to the start of the tuple would provide an alternative measure for sorting
    matches.
    """
    r = []
    initial_length = len(a)
    while a and b:
        #print("matching {} in {}".format(a, b))
        matched = longest_matched_sequence(a, b)
        #print("-> '{}'".format(matched))
        if matched is None:
            break
        start_a, start_b, length = matched
        r.append(length / initial_length)
        
        # Remove the matched sequences before looking for further matches
        a = a[:start_a] + a[start_a+length:]
        b = b[:start_b] + b[start_b+length:]
    return tuple(r)


def longest_matched_sequence(a, b):
    """ Finds longest sequence in a also found in b
    Returns the starting index of the sequence in a and the starting index
    for the same sequence in b. In case of ties it will return
    the first found in a and its first occurrence in b. """
    for length in range(len(a), 0, -1):
        for start_a in range(len(a) - length + 1):
            for start_b in range(len(b) - length + 1):
                if a[start_a:start_a+length] == b[start_b:start_b+length]:
                    return start_a, start_b, length
    return None


def loose_key_match(search_parts, key_parts):
    """ Test whether a list of search strings e.g. ["rofst", "f"] matches a key
    such as ["rofst_phh", "gpia", "3", "q2", "urb"], with a loose algorithm that includes
    cases where the parts of the search string are found within the parts of
    the key. """
    # First, quick test of whether all parts are found
    if any(not any(s in k for k in key_parts) for s in search_parts):
        return False
    
    # Then slower test of whether the parts are found in a specific permutation
    for permutation in permutations(key_parts, len(search_parts)):
        if all(s in k for s, k in zip(search_parts, permutation)):
            return True
    return False


def strict_key_match(search_parts, key_parts):
    """ Test whether a list of search strings e.g. ["rofst", "f"] matches a key
    such as ["rofst_phh", "gpia", "3", "q2", "urb"], with a stricter algorithm that includes
    only cases where the parts of the search string match exactly parts of
    the key. """
    if any((s not in key_parts) for s in search_parts):
        return False
    
    return tuple(search_parts) in permutations(key_parts, len(search_parts))


def get_root(short_key, all_short_keys):
    for ancestor in get_relations(short_key, all_short_keys, is_ancestor):
        if not has_ancestor(ancestor, all_short_keys):
            return ancestor

    
def has_ancestor(short_key, all_short_keys):
    return any(True 
               for _ in get_relations(short_key, all_short_keys, is_ancestor))
    
    
def get_relations(short_key, all_short_keys, relation, reverse=False):
    """ Get all indicator short keys for which the relation function is True """
    me = short_key.split("-")
    predicate = relation
    if reverse:
        predicate = lambda a, b: relation(b, a)
    for other in all_short_keys:
        if predicate(other.split("-"), me):
            yield other
                
            
def is_ancestor(a, b):
    """ Whether a is an ancestor of b, i.e. by disaggregating a one or more
    times you get b.
    Both a and b are represented as lists of parts of the short key.
    Note, this assumes there are no duplicate parts in short keys, which 
    is the case for the current UIS set. """
    return a[0] == b[0] and set(b) > set(a)
    # return a[0] == b[0] and len(a) < len(b) and all((part in b) for part in a)

    
def is_parent(a, b):
    """ Whether a is a parent of b, i.e. by disaggregating a once you get b. """
    return is_ancestor(a, b) and len(set(b) - set(a)) == 1
  
    
def specs_match(incomplete_spec, indicator):
    """ Whether incomplete spec is a potential match for an indicator """
    for k, v in incomplete_spec.items():
        if v not in [None, "", indicator[k]]:
            return False
    return True


def latest_by_country(df):
    """
    Find the latest data point by country in a dataframe from UIS API
    Also adds a 'country' column with the country name and the year of the latest
    data.
    """    
    group = ["Indicator key", "REF_AREA"]
    df = df.sort_values(by="TIME_PERIOD").groupby(group, as_index=False).last()
    df["Country"] = formatted_column(df, "{UN country name} ({Year})")
    return df
    
def formatted_column(dataframe, format_string):
    """ Create a new string column using the format template and the other 
        columns in each row
    """
    return dataframe.apply(lambda r: format_string.format(**r), axis=1)
    