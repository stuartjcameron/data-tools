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
    

    
@author: scameron
"""

import inflection
import requests
from urllib.parse import urljoin
import logging as lg
from collections import defaultdict
lg.basicConfig(level=lg.DEBUG)
LONG = object()
WIDE = object()

class Filter(object):
    """ Class for managing the dimensions that make up an SDMX 'indicator',
    and other dimensions that can be used to filter API queries.
    
    We assume the other dimensions are ref_area (country) and time_period (year)
    """
    non_ind = [
        "REF_AREA",
        "TIME_PERIOD"
        ]       # assuming these are the same across SDMX data flows!
    
    def __init__(self, dimensions):
        self.all_dims = dimensions
        self.ind_dims = [d for d in dimensions if not d in self.non_ind]
    
    def dims(self, ind=True):
        if ind:
            return self.ind_dims
        else:
            return self.all_dims
        
    def is_dim(self, dim, ind=True):
        return dim.upper() in self.dims(ind)
    
    def is_complete(self, d):
        """ Test whether a dictionary represents a complete indicator """
        return all(d[k] for k in self.dims())
    
    def extract_dims(self, d, ind=True):
        """ Extract the valid dimensions from a dictionary """
        return {k: v for k, v in d.items() if self.is_dim(k, ind)}
    
    def extract_dims_and_remainder(self, d, ind=True):
        r = {}
        remainder = {}
        for k, v in d.items():
            if self.is_dim(k, ind):
                r[k] = v
            else:
                remainder[k] = v
        return r, remainder
    
    def dict_to_key(self, d=None, ind=True):
        """ Returns an SDMX key for the set of dimensions represented
        by the dictionary d. If ind=False it may also return ref_area
        and time_period (if these are in the dict)
        """ 
        if d:
            return ".".join(value_to_filter_string(d[k])
                        for k in self.dims(ind))
        else: # d not specified, None or {} -- empty query
            return "." * (len(self.dims(ind)) - 1)
            
    def key_to_dict(self, key, ind=True):
        """ Returns a dict based on an SDMX key """
        return self.extract_dims((zip(self.all_dims, key.split("."))))
    
def combine_queries(*query_dicts):
    """ Combine dicts representing indicators or queries into a 
    single dict with multiple values.
    Note this can 'over-query', i.e. return too many results if there
    is too much difference between the queries. """
    r = defaultdict(set)
    for d in query_dicts:
        for key, value in d.items():
            if type(value) is list:
                r[key] |= set(value)
            else:
                r[key].add(value)
    return {k: list(v) for k, v in r.items()}
 
def value_to_filter_string(v):
    """ Convert None, a number, string or list into a string for 
    inclusion in an SDMX filter string """
    if v is None:
        return ""
    elif type(v) == list:
        return "+".join(v)
    else:
        return str(v)

def camel(k):
    """ Appropriately camelize a keyword argument key for inclusion in 
    an SDMX URL query 
    e.g. start_period => startPeriod 
    """
    return inflection.camelize(k, uppercase_first_letter=False)


class Api(object):
    def __init__(self, url, subscription_key, dimensions=None):
        self.url = url
        self.subscription_key = subscription_key
        self.filter = Filter(dimensions)
        
    def get_dimensions(self):
        """ Get the list of dimensions from the API in the right order """
        # TODO: add this!
    
    def get_data_for_spec(self, spec, **params):
        #TODO: deal with ref_area and time_period
        url = urljoin(self.url, self.filter.dict_to_key(spec))
        params = {camel(k): v for k, v in params.items()}
        params["format"] = "sdmx-json"
        return requests.get(url, params=params).json()
    
    def get_all_data(self, **params):
        """ Gets data without filtering """
        return self.get_data_for_spec({}, **params)
        
    def get_data(self, **kwargs):
        """ Use kwargs to make filter and pass remainder as parameters """
        spec, remainder = self.filter.extract_dims_and_remainder(kwargs)
        return self.get_data_for_spec(spec, remainder)
    
    def get_possible_values(self):
        """ Submit a keys-only request to find out range of values in each
        dimension """
        message = self.get_all_data(detail="serieskeysonly", 
                                    dimension_at_observation='AllDimensions')
        dimensions = message["structure"]["dimensions"]["observation"]
        return {d["id"]: [v["id"] for v in d["values"]] for d in dimensions}
    
    def get_possible_values_for_spec(self, spec):
        """ Submit a keys-only request to find out what range of other dimensions
        are available within the fields specified by the spec
        Returns a dictionary e.g. {"REF_AREA": ["BD", "IN"...]} 
        
        Note: returns an empty list for the time period. This information is not
        available in the query. Presumably need to obtain the actual data to get this?
        """
        message = self.get_data_for_spec(spec=spec,
                                    detail="serieskeysonly", 
                                    dimension_at_observation='AllDimensions')
        dimensions = message["structure"]["dimensions"]["observation"]
        return {d["id"]: [v["id"] for v in d["values"]] for d in dimensions}
    
    def match_inds(self, spec=None):
        """ Recursive generator to find all indicators that are in 
        the API and fit the given indicator specification dict. 
        Yields the indicators as dicts of dimensions and values.
        
        Not specifying spec will return all indicators available in the API
        """
        if spec is None:
            spec = {}
        else:
            spec = self.filter.extract_dims(spec)
        message = self.get_dimensions_for_spec(spec)
        dims = message["structure"]["dimensions"]["observation"]
        undetermined = {}
        determined = {}
        for dim in dims:
            dim_id = dim["id"]
            value_ids = [v["id"] for v in dim["values"]]
            if self.filter.is_dim(dim_id):
                if len(value_ids) == 0: # shouldn't happen
                    raise ValueError("No values found for {}".format(dim_id))
                elif len(value_ids) == 1:
                    determined[dim_id] = value_ids[0]
                else:
                    undetermined[dim_id] = value_ids    
        if not undetermined:
            yield determined
        else:
            first_dim, first_value_ids = next(iter(undetermined.items()))
            for value_id in first_value_ids:
                determined[first_dim] = value_id
                if len(undetermined) == 1:
                    yield determined
                else: # >1 undetermined value, so we need to do another query
                    for ind in self.match_inds(spec):
                        yield ind

    def match_inds_to_file(self, filename, spec=None):
        """ Find indicators using match_inds and write them to a file """
        with open(filename, "w") as f:
            for i, d in enumerate(self.match_inds(spec)):
                s = self.filter.dict_to_key(spec)
                lg.info("{}: {}".format(i, s))
                f.write("{}\n".format(s))

def sdmx_to_icy(message, filt, meta=False):
    """ Convert an SDMX message containing data to indicator, country, year 
    json format. Optionally, the format can contain metadata and notes. 
    TODO: check if there are cases when >1 dataset is returned! 
    TODO: allow indicator shorthands... based on a lookup from a CSV
    TODO: make it part of the api? rather than having to add the filter
    ... or get the dict_to_key function from the message itself to allow
    it to be freestanding.
    TODO: add the full indicator metadata at the end of the message if meta=True
    """
    obs = message["dataSets"][0]["observations"]
    dimensions = message["structure"]["dimensions"]["observation"]
    dimensions = [(d["id"], d["values"]) for d in dimensions]
    attributes = message["structure"]["attributes"]["observation"]
    attributes = [(d["id"], d["values"]) for d in attributes]
    
    # Return an ind: {country: {year: value}} nested dictionary
    r = defaultdict(lambda: defaultdict(dict))
    for k, v in obs.items():
        # We convert a numberical key like 0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0
        # into a dictionary of dimensions and then back into a more readable
        # string like NERA.PT.M..., 
        dim_dict = {}
        for dimension_value, (id_, values) in zip(k.split(":"), dimensions):
            dim_dict[id_] = values[int(dimension_value)]["id"]
        indicator_id = filt.dict_to_key(dim_dict)
        # ignoring attributes for now - add the exception checking later!
        ref_area = dim_dict["REF_AREA"]
        time_period = dim_dict["TIME_PERIOD"]
        r[indicator_id][ref_area][time_period] = v[0]
    return r
    
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


Note, no longer using these but might be useful for querying dataflow...
defaults = {
    "data": {},
    "dataflow": {"references": "datastructure"}
}
    
templates = {
    "data": "data/UNESCO,EDU_NON_FINANCE,3.0/{filters}",
    "dataflow": "dataflow/UNESCO/EDU_NON_FINANCE/latest"
}


"""    