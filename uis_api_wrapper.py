# -*- coding: utf-8 -*-
"""
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

LONG = object()
WIDE = object()


const_params = {
    "subscription-key": "8be270194d6444189bdde1a7b2666911",
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

dimension_ids = [
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


def construct_query(query_type, quiet=False, **kwargs):
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
    if not quiet:
        print("- yr url is {url} with params {params}".format(url=url, params=params))
    return url, params

def get_json(query_type, quiet=False, **kwargs):
    """ Construct a URL based on the query type and kwargs
    and return the JSON found there """
    if not quiet:
        print("getting json", query_type, kwargs)
    url, params = construct_query(query_type, quiet=quiet, **kwargs)
    return requests.get(url, params=params).json()
    
# See here https://apiportal.uis.unesco.org/query-builder for example queries:
# e.g. https://api.uis.unesco.org/sdmx/data/UNESCO,EDU_NON_FINANCE,2.0/OFST+ROFST+ROFST_PHH.PT+PER.L1+L2._T....._Z..._T.........?format=sdmx-json&startPeriod=2012&endPeriod=2017&locale=en&subscription-key=8be270194d6444189bdde1a7b2666911
# (but this query is too large to return! Need to set countries)
# https://api.uis.unesco.org/sdmx/data/UNESCO,EDU_NON_FINANCE,2.0/OFST+ROFST+ROFST_PHH.PT+PER.L1+L2._T....._Z..._T.........AIMS_EAS_PAC?format=sdmx-json&startPeriod=2012&endPeriod=2017&locale=en&subscription-key=8be270194d6444189bdde1a7b2666911


def make_filters(**kwargs):
    r = []
    for d in dimension_ids:
        if d.lower() in kwargs:
            v = kwargs[d.lower()]
            if type(v) == list:
                r.append('+'.join(v))
            else:
                r.append(str(v))
        else:
            r.append("")  
    return ".".join(r)

def get_data(quiet=False, **kwargs):
    """ Make filters and pass remaining parameters to the json query
    TODO: TIME_PERIOD cannot be coded here 
    """
    kwargs = {k.lower(): v for k, v in kwargs.items()}
    filters = []
    for d in dimension_ids:
        d = d.lower()
        if d in kwargs:
            v = kwargs[d]
            if type(v) == list:
                filters.append('+'.join(v))
            else:
                filters.append(str(v))
            del(kwargs[d])
        else:
            filters.append("")  
    
    kwargs = {inflection.camelize(k, uppercase_first_letter=False): v
              for k, v in kwargs.items()}
    return get_json("data", quiet=quiet, filters='.'.join(filters), **kwargs)

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

"""    
    