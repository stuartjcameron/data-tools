# -*- coding: utf-8 -*-
"""
Access the UNESCO Institute of Statistics SDMX API, download data
in a convenient format, and find information on indicators that are available
in the database.

@author: scameron
"""

import sdmx_api
import translate_sdmx
import csv
from itertools import permutations
try:
    from hdx.location.country import Country
except ImportError:
    pass

import logging as lg
lg.basicConfig(level=lg.DEBUG)

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
           "REF_AREA",
           "TIME_PERIOD"
            ]
uis_filter = sdmx_api.Filter(UIS_DIMENSIONS)
INDICATOR_DATA = "input-data/combined indicators.csv"


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
    key_types = ["key", "short_key", "Indicator ID"]
    keys = {k: [] for k in key_types}
    with open(INDICATOR_DATA, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            for t in key_types:
                keys[t].append(row[t])
    keys["id"] = keys["Indicator ID"]
    keys["id_lower"] = [k.lower() for k in keys["id"]]
    keys["key_lower"] = [k.lower() for k in keys["key"]]
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


class Api(sdmx_api.Api):
    """ 
    Access data from the UNESCO Institute of Statistics and return it in a 
    convenient format.
    
    This is a tailored version of the more generic sdmx_api.
    """

    def __init__(self, subscription_key):
        super(Api, self).__init__(base=UIS_BASE,
                                subscription_key=subscription_key,
                                dimensions=UIS_DIMENSIONS)
        
    def quick_query(self, ind, country=None, start=None, end=None,
                       use_live_country_info=False, **kwargs):
        query = kwargs
        if ind:
            query = {**kwargs, **Indicator.fuzzy_lookup(ind).spec}
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
        return self.query(dimension_at_observation="AllDimensions", **query)
    
    def icy_query(self, ind, country=None, start=None, end=None, 
               use_live_country_info=False,
               **kwargs):
        """
        Query UIS API based on an indicator specification dictionary or string
        (which will be retrieved using a fuzzy lookup); optionally, country or list
        of countries (also fetched using fuzzy lookup); and optionally,
        start and end years.
        
        If hdx.location.country is not available, then fuzzy country lookup is not
        done and the countries can only be specified as a list of ISO2 codes
        e.g. ["UG", "BD"]
        
        If use_live_country_info is True, it will request the hdx country
        module to get the latest country data from the web. (This slows
        everything down and country information changes infrequently,
        so is disabled by default.)
        
        Additional arguments will be passed to the API request.
        
        TODO: allow multiple indicator requests
        """
        message = self.quick_query(ind, country, start, end, use_live_country_info, **kwargs)
        result = translate_sdmx.to_icy(message)
        r = {}
        for k, v in result.items():
            if k == "metadata":
                r[k] = v
            else:
                r[Indicator(k).id] = v
        return r
    
    def df_query(self, ind, country=None, start=None, end=None,
                 use_live_country_info=False, **kwargs):
        """
        Query UIS API based on an indicator specification dictionary or string
        (which will be retrieved using a fuzzy lookup); optionally, country or list
        of countries (also fetched using fuzzy lookup); and optionally,
        start and end years.
        
        If hdx.location.country is not available, then fuzzy country lookup is not
        done and the countries can only be specified as a list of ISO2 codes
        e.g. ["UG", "BD"]
        
        If use_live_country_info is True, it will request the hdx country
        module to get the latest country data from the web. (This slows
        everything down and country information changes infrequently,
        so is disabled by default.)
        
        Additional arguments will be passed to the API request.
        
        Returns a Pandas dataframe with useful information added from the 
        country and indicator databases.
        
        """
        message = self.quick_query(ind, country, start, end, use_live_country_info, **kwargs)
        return message
        df = translate_sdmx.to_df(message)
        df = add_country_info_to_df(df, {
                "#country+name+preferred": "country name",
                "#country+alt+i_en+name+v_unterm": "UN country name",
                "#region+name+preferred+sub": "region"                
                })
        df = add_indicator_info_to_df(df, [
                'Indicator Label - EN', 
                'Indicator Section', 
                'Table query', 
                'Theme'
                ])
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

def add_info_to_df_OLD(left, right, fields, on):
    """ Adds information from right dataframe to the left. 
    Fields can be a list - if column in the 'to' df is to be the same as
    in the 'from' df - or dict - allowing the column name to change.
    on should be a column name in the left df and the index of the right df. 
    
    No longer used - new version using df.merge instead. Disadvantage is that it doesn't
    do the merge in place
    """
    try:
        lookup = fields.items()
    except AttributeError:
        lookup = zip(fields, fields)
    for column_in_left, column_in_right in lookup:
        left[column_in_left] = left[on].apply(right[column_in_right])

    
    
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
    
    
    def __init__(self, key=None, id=None, short_key=None, index=None):
        if index is None:
            index = self._get_index(key, id, short_key)
        self.key, self.short_key, self.id = (self.keys[t][index]
            for t in self.key_types)
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
    def fuzzy_lookup(cls, s, shortest=True, allow_multiple=False):
        """
        Finds one or more indicators matching the given string query.
        Looks first for an exact match by ID, key or short key.
        
        If that fails it looks for keys, short keys or IDs that contain
        all parts (separated by spaces) of the string.
        
        Returns a single indicator where possible, or a list where there
        are multiple matches if allow_multiple=True.
        """
        
        s = s.lower()
        # First look for an exact match
        if s in cls.keys["key_lower"]:
            return cls(key=s)
        if s in cls.keys["id_lower"]:
            return cls(id=s)
        if s in cls.keys["short_key"]:
            return cls(short_key=s)
        
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
            if indices:
                matches = [cls(index=i) for i in indices]
                if shortest:
                    min_length = min((m.parts(), len(m)) for m in matches)
                    matches = [m for m in matches 
                               if (m.parts(), len(m)) == min_length]
                if len(matches) == 1:
                    return matches[0]
                elif allow_multiple:
                    return matches
            
        return None
    
    
    @classmethod
    def match_spec(cls, spec):
        """ List all indicators in the dictionary that fit a given specification """
        def gen():
            for key in cls.keys["key"]:
                ind = uis_filter.key_to_dict(key)
                if specs_match(spec, ind):
                    yield cls(key=key)
                    
        return list(gen())
              
    
    @classmethod
    def match(cls, **kwargs):
        return cls.match_spec({k.upper(): v for k, v in kwargs.items()})
            
        
    def parts(self):
        """ Number of parts of the short key """
        return len(self.short_key.split("-"))
    
    def __len__(self):
        return len(self.short_key)    
    
    def matches_spec(self, spec):
        """ Whether or not a given spec matches the indicator """
        return specs_match(spec, self.spec)
    
    
    def __repr__(self):
        return "<UIS Indicator {}>".format(self.id)
    
    

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
        
    
def specs_match(incomplete_spec, indicator):
    """ Whether incomplete spec is a potential match for an indicator """
    for k, v in incomplete_spec.items():
        if v not in [None, "", indicator[k]]:
            return False
    return True

def latest_by_country(df):
    group = ["Indicator key", "REF_AREA"]
    return df.sort_values(by="TIME_PERIOD").groupby(group, as_index=False).last()
    
