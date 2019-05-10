# -*- coding: utf-8 -*-
"""
Request data from an SDMX API and return it in a convenient
'indicator-country-year' JSON or Pandas dataframe format.

This has been designed for use with the uis api


Functions that wrap requests to the UIS data API
Currently contains fairly low level functions that simply convert a 
python function call to a request to the API and return the data.

Example: Get adjusted net enrolment rate for Bangladesh and Uganda,
2012-16.

    nera_bd = api.get_data(stat_unit="NERA",
         ref_area=["BD", "UG"],
         unit_measure="PT",
         start_period=2012,
         end_period=2016,
         dimension_at_observation='AllDimensions')
    
TODO: make this into a class that can be initiated with a subscription
key. 
    
@author: scameron
"""

import inflection
import requests
from urllib.parse import urljoin
from string import Formatter
import logging as lg
lg.basicConfig(level=lg.DEBUG)
LONG = object()
WIDE = object()

class Spec(object):
    """ UIS indicator specification
    
    Class for storing dimension-value pairs that relate to a UIS indicator
    Can be passed to an API query to get all data that matches the specification
    - may be one or several indicators
    
    """
    ignore = [
        "REF_AREA",
        "TIME_PERIOD"
        ]

    def __init__(self, **kwargs):
        self.parameters = {}
        for k, v in kwargs.items():
            if self.is_dimension(k):
                self.parameters[k.upper()] = v
            else:
                raise TypeError("Unexpected keyword argument '{k}'".format(k))
    
    @classmethod
    def is_dimension(self, dim):
        return (dim.upper() in DIMENSIONS 
                and not dim.upper() in self.ignore)
        
    @classmethod
    def extract_from(cls, d):
        """ Make a specification from any appropriate keywords and return
        a dict from the irrelevant keywords """
        spec = cls()
        irrelevant = {}
        for k, v in d.items():
            if cls.is_dimension(k):
                spec.parameters[k] = v
            else:
                irrelevant[k] = v
        return spec, irrelevant
                
    
    @classmethod
    def from_key(cls, k):
        """ Initialize a specification from an SDMX key """
        return cls(**dict(zip(DIMENSIONS, k.split("."))))

    @classmethod
    def dims(cls):
        return [d for d in DIMENSIONS if d not in cls.ignore]
    
                
    def get(self, d):
        """ Get the relevant dimension, case-insensitive """
        if self.is_dimension(d):
            return self.parameters.get(d.upper(), None)
        else:
            raise KeyError()
    
    def is_complete(self):
        return all(self.parameters[k] 
                   for k in DIMENSIONS
                   if not k in self.ignore)
    
    def to_dict(self):
        return self.parameters
        
    def to_key(self, ref_area=False, time_period=False):
        """ Return the SDMX key for this indicator. Optionally includes
        ref_area and time_period. """
        r = []    
        for dimension_id in DIMENSIONS:
            if not((dimension_id == "REF_AREA" and ref_area is False) or
                   (dimension_id == "TIME_PERIOD" and time_period is False)):
                
                v = self.get(dimension_id)
                if v is None:
                    r.append("")
                elif type(v) == list:
                    r.append('+'.join(v))
                else:
                    r.append(str(v))
        return ".".join(r)
    
    def to_filters(self):
        """ Returns a filter string based on the SDMX parameters """
        return '.'.join(value_to_filter_string(self.get(d))
                        for d in DIMENSIONS)

def value_to_filter_string(v):
    """ Convert any value into a string for inclusion in an SDMX
    filter string """
    if v is None:
        return ""
    elif type(v) == list:
        return "+".join(v)
    else:
        return str(v)

const_params = {
    "format": "sdmx-json"
}

root_url = "http://api.uis.unesco.org/sdmx/"
defaults = {
    "data": {},
    "dataflow": {"references": "datastructure"}
}
    
templates = {
    "data": "data/UNESCO,EDU_NON_FINANCE,3.0/{filters}",
    "dataflow": "dataflow/UNESCO/EDU_NON_FINANCE/latest"
}

def construct_query(query_type, **kwargs):
    """ Create a URL based on templates, defaults and root_url above and
    fields specified as kwargs. kwargs that are present as string fields in 
    the URL template will be added first. 
    Any remaining kwargs will be added as URL query parameters.
     """
    
    # replace kwargs found in templates
    url_end = templates[query_type]
    url = urljoin(root_url, url_end.format_map(kwargs))
    fields = [fname for _, fname, _, _ in Formatter().parse(url_end) if fname]
    remaining = {k: v for k, v in kwargs.items() if not k in fields}
    params = {**const_params, **defaults[query_type], **remaining}
    lg.info("- yr url is {url} with params {params}".format(url=url, params=params))
    return url, params


class Api(object):
    def __init__(self, url, subscription_key, dimensions=None):
        self.url = url
        self.subscription_key = subscription_key
        self.dimensions = dimensions
        
    def get_dimensions():
        """ Look up the dimensions from the API if they are not provided
        by the user """
        #TODO
        
        
def get_json(query_type, **kwargs):
    """ Construct a URL based on the query type and kwargs
    and return the JSON found there """
    lg.info("getting json", query_type, kwargs)
    url, params = construct_query(query_type, **kwargs)
    return requests.get(url, params=params).json()
    
# See here https://apiportal.uis.unesco.org/query-builder for example queries:
# e.g. https://api.uis.unesco.org/sdmx/data/UNESCO,EDU_NON_FINANCE,2.0/OFST+ROFST+ROFST_PHH.PT+PER.L1+L2._T....._Z..._T.........?format=sdmx-json&startPeriod=2012&endPeriod=2017&locale=en&subscription-key=8be270194d6444189bdde1a7b2666911
# (but this query is too large to return! Need to set countries)
# https://api.uis.unesco.org/sdmx/data/UNESCO,EDU_NON_FINANCE,2.0/OFST+ROFST+ROFST_PHH.PT+PER.L1+L2._T....._Z..._T.........AIMS_EAS_PAC?format=sdmx-json&startPeriod=2012&endPeriod=2017&locale=en&subscription-key=8be270194d6444189bdde1a7b2666911

def get_data_for_spec(spec, params):
    params = {inflection.camelize(k, uppercase_first_letter=False): v
              for k, v in params.items()}
    return get_json("data", filters=spec.to_filters(), **params)

def get_data(**kwargs):
    """ Make filters and pass remaining parameters to the json query
    TODO: TIME_PERIOD cannot be coded here 
    """
    spec, remainder = Spec.extract_from(kwargs)
    return get_data_for_spec(spec, remainder)

def get_dimensions_alt(**kwargs):
    """ Submit a keys-only request to find out what range of other dimensions
    are available within the fields specified by the kwargs 
    Returns a dictionary e.g. {"REF_AREA": ["BD", "IN"...]} 
    
    NOTE: The new version of this function seems to return the same results
    and is quicker. May be worth checking against this old version
    occasionally.
    """
    message = get_data(detail="serieskeysonly", **kwargs)
    #print("message length - v1", len(json.dumps(message)))
    obs = message["dataSets"][0]["series"]
    dimensions = message["structure"]["dimensions"]["series"]
    dimensions = [(d["id"], d["values"]) for d in dimensions]
    r = {id_: set() for (id_, _) in dimensions}
    for k, v in obs.items():
        for dimension_value, (id_, values) in zip(k.split(":"), dimensions):
            r[id_].add(values[int(dimension_value)]["id"])
    
    return {k: list(v) for k, v in r.items()}
    
    
def get_dimensions(**kwargs):
    """ Submit a keys-only request to find out what range of other dimensions
    are available within the fields specified by the kwargs 
    Returns a dictionary e.g. {"REF_AREA": ["BD", "IN"...]} 
    
    Note: returns an empty list for the time period. This information is not
    available in the query. Presumably need to obtain the actual data to get this?
    """
    message = get_data(detail="serieskeysonly", dimension_at_observation='AllDimensions', **kwargs)
    dimensions = message["structure"]["dimensions"]["observation"]
    return {d["id"]: [v["id"] for v in d["values"]] for d in dimensions}

def sdmx_to_icy(message, meta=False):
    """ Convert an SDMX message containing data to indicator, country, year 
    json format. Optionally, the format can contain metadata and notes. """

    
def sdmx_to_df(message):
    """ Convert a UIS SDMX message containing data to a pandas dataframe
        The dataframe will be structured in long format:
        ref_area | time_period | indicator | value in columns
        """
        
        
        
"""
TODO:
- consider functions to reshape dataframes:
    - TIME_PERIOD: ref_area | indicator | 1995 | 1996 etc. for 1+ indicators
    - STAT_UNIT: unit" -- wide: country | year | NERA | NARA etc. for 1+ years
    - "combined" -- Combined wide: country | NERA_1995 | NERA_1996 etc. | NARA_1995 | NARA_1996...
    
(This should cover most cases. Rarely want countries as column heading)
Note hdf offers a way of progressively appending bits of data to a single
dataframe on disk http://pandas-docs.github.io/pandas-docs-travis/user_guide/io.html#table-format

- make main functions into a class that allows user to set subscription-key, e.g.
import uis_api
api = uis_api.Api(subscription_key="...")


2019.05.10
There are now 2 main basic modules:
    uis_api_wrapper - wraps the API itself
    uis_indicators - wraps a dictionary of indicators
    
Plus a script 'make_uis_indicator_dict' which documents the creation of the consolidated dictionary
of indicators.

Next, make a higher level module that imports both of these.


Note that I haven't used the dataflow api at all and am not sure how useful
this is! So we can simplify some of this.

Also consider what can be abstracted from this for more general case.
Possible to have something that can get any SDMX data containing ind, country, year?
(And then generalising to ind, any geographical reference year, any type of time period)

Something like:
    import icy_sdmx
    uis_api = icy_sdmx.Api("http://api.uis.unesco.org/sdmx/data/UNESCO,EDU_NON_FINANCE,3.0",
                                subscription_key="...")
    uis_api.set_indicator_dimensions(["STAT_UNIT",
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
       "IMM_STATUS"])

Assuming that ref_area and time_period are universal for now. If they differ
then may have to use different names for these.

"""    