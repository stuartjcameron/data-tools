# -*- coding: utf-8 -*-
"""
Class wrapping a dictionary of labels for UIS indicators
@author: scameron
"""

import icy_sdmx
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

uis_filter = icy_sdmx.Filter(UIS_DIMENSIONS)

def get_iso2(s, use_live=False):
    if Country:
        iso3, fuzzy = Country.get_iso3_country_code_fuzzy(s, use_live)
        if iso3:
            return Country.get_country_info_from_iso3(iso3)["#country+code+v_iso2"]
    return s.upper()

def get_indicator_keys():
    key_types = ["key", "short_key", "Indicator ID"]
    keys = {k: [] for k in key_types}
    with open("input-data/combined indicators.csv", "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            for t in key_types:
                keys[t].append(row[t])
    keys["id"] = keys["Indicator ID"]
    keys["id_lower"] = [k.lower() for k in keys["id"]]
    keys["key_lower"] = [k.lower() for k in keys["key"]]
    return keys
            
class Api(icy_sdmx.Api):
    def __init__(self, subscription_key):
        super(Api, self).__init__(base=UIS_BASE,
                                subscription_key=subscription_key,
                                dimensions=UIS_DIMENSIONS)
        
    def qquery(self, ind, country=None, start=None, end=None, 
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
        """
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
        result = self.icy_query(**query)
        r = {}
        for k, v in result.items():
            if k == "metadata":
                r[k] = v
            else:
                r[Indicator(k).id] = v
        return r
        
     
class Indicator(object):
    """ UIS indicator 
    
    Wraps a dictionary of indicator labels and specifications with convenient
    lookup functions
    
    """
    keys = get_indicator_keys()
    key_types = ["key", "short_key", "id"]
    
    def __init__(self, key=None, id=None, short_key=None, index=None):
        if index is None:
            index = self.get_index(key, id, short_key)
        self.key, self.short_key, self.id = (self.keys[t][index]
            for t in self.key_types)
        self.spec = uis_filter.key_to_dict(self.key)
    
    @classmethod
    def get_index(cls, key=None, id=None, short_key=None):
        if key:
            return cls.keys["key_lower"].index(key.lower())
        elif id:
            return cls.keys["id_lower"].index(id.lower())
        elif short_key:
            return cls.keys["short_key"].index(short_key.lower())
        
    @classmethod
    def fuzzy_lookup(cls, s, shortest=True, allow_multiple=False):
        """ Finds one or more indicators matching the given string query.
        Looks first for an exact match by ID, key or short key.
        If that fails it looks for keys, short keys or IDs that contain
        all parts (separated by spaces) of the string.
        Returns a single indicator where possible, or a list where there
        are multiple matches.
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
            matches = [cls(index=i) for i in indices]
            if shortest:
                min_length = min(len(match) for match in matches)
                matches = [m for m in matches if len(m) == min_length]
            if len(matches) == 1:
                return matches[0]
            elif allow_multiple:
                return matches
            
        return None
    
    @classmethod
    def match(cls, **kwargs):
        return cls.match_spec({k.upper(): v for k, v in kwargs.items()})
        
    @classmethod
    def match_spec(cls, spec):
        """ List all indicators in the dictionary that fit a given specification """
        def gen():
            for key in cls.keys["key"]:
                ind = uis_filter.key_to_dict(key)
                if specs_match(spec, ind):
                    yield cls(key=key)
                    
        return list(gen())
                
    def __len__(self):
        return len(self.short_key.split("-"))
    
    def matches_spec(self, spec):
        return specs_match(spec, self.spec)
    
    def __repr__(self):
        return self.short_key

def loose_key_match(search_parts, key_parts):
    """ Test whether a search string e.g. "rofst f" matches a key
    such as "rofst_phh-gpia-3-q2-urb", with a loose algorithm that includes
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
    """ Test whether a search string e.g. "rofst f" matches a key
    such as "rofst_phh-gpia-3-q2-urb", with a stricter algorithm that includes
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

