# -*- coding: utf-8 -*-
"""
Request data from an SDMX API and return it in a convenient
'indicator-country-year' JSON or Pandas dataframe format.

This has been designed for use with the uis api


Functions that wrap requests to the UIS data API
Currently contains fairly low level functions that simply convert a 
python function call to a request to the API and return the data.

    
@author: scameron
"""

import inflection
import requests
from urllib.parse import urljoin
import logging as lg
from collections import defaultdict, Counter
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
        return {k.upper(): v for k, v in d.items() if self.is_dim(k, ind)}
    
    def extract_dims_and_remainder(self, d, ind=True):
        r = {}
        remainder = {}
        for k, v in d.items():
            if self.is_dim(k, ind):
                r[k.upper()] = v
            else:
                remainder[k] = v
        return r, remainder
    
    def dict_to_key(self, d=None, ind=True):
        """ Returns an SDMX key for the set of dimensions represented
        by the dictionary d. If ind=False it may also return ref_area
        and time_period (if these are in the dict)
        """ 
        if d:
            return ".".join(value_to_filter_string(d.get(k))
                        for k in self.dims(ind))
        else: # d not specified, None or {} -- empty query
            return "." * (len(self.dims(ind)) - 1)
            
    def key_to_dict(self, key, ind=True):
        """ Returns a dict based on an SDMX key """
        return self.extract_dims(dict((zip(self.all_dims, key.split(".")))))
    
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
    def __init__(self, base, subscription_key, dimensions=None):
        self.base = base
        if base[-1] != "/":
            self.base += "/"
        self.subscription_key = subscription_key
        if dimensions:
            self.filter = Filter(dimensions)
        self.verification = True
        
    def get(self, spec=None, params=None):
        """ Forms a URL from the filter specification and submits a request
        to the API using the specified parameters. Returns a message in json
        format. """
        if params is None:
            params = {}
        url = urljoin(self.base, self.filter.dict_to_key(spec, False))
        params = {camel(k): v for k, v in params.items()}
        params["format"] = "sdmx-json"
        params["subscription-key"] = self.subscription_key
        lg.info("Api.get \nurl:%s \nparams:%s", url, params)
        return requests.get(url, params=params, verify=self.verification).json()
    
    def query(self, **kwargs):
        """ Convenience function for querying the API. Accepts any parameter
        that can filter the query, and passes the remainder as parameters to 
        the API request.
        """
        spec, remainder = self.filter.extract_dims_and_remainder(kwargs, False)
        return self.get(spec, remainder)
    
    def icy_query(self, **kwargs):
        """ Query the API and return data in indicator-country-year json
        format. """
        message = self.query(dimension_at_observation="AllDimensions",
                             **kwargs)
        return sdmx_to_icy(message)
        
    def get_dimension_information(self, spec=None):
        """ Get the dimension information from the API for the given spec """
        params = {"detail": "serieskeysonly", 
                 "dimension_at_observation": "AllDimensions"}
        message = self.get(spec, params)
        return message["structure"]["dimensions"]["observation"]
        
    def get_filter(self):
        """ Get the list of dimensions from the API in the right order """
        dimensions = [d["id"] for d in self.get_dimension_information()]
        self.filter = Filter(dimensions)
    
    def scope(self, spec=None):
        """  
        Submit a keys only request to the API and return the values found
        (within the specified filter, if any) for each dimension. 
        
        Returns a dictionary e.g. {"REF_AREA": ["BD", "IN"...], ...} 
        
        Note: returns an empty list for the time period. This information is not
        available in the keys-only query.
        """
        return {d["id"]: [v["id"] for v in d["values"]] 
                for d in self.get_dimension_information(spec)}
    
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
        undetermined = {}
        determined = {}
        for dim_id, value_ids in self.scope(spec).items():
            if self.filter.is_dim(dim_id):
                if len(value_ids) == 0: # shouldn't happen
                    raise ValueError("No values found for {}".format(dim_id))
                elif len(value_ids) == 1:
                    determined[dim_id] = value_ids[0]
                else:
                    undetermined[dim_id] = value_ids    
        if undetermined:
            first_dim, first_value_ids = next(iter(undetermined.items()))
            for value_id in first_value_ids:
                determined[first_dim] = value_id
                if len(undetermined) == 1:
                    yield determined
                else: # >1 undetermined value, so we need to do another query
                    yield from self.match_inds(spec=determined)
                    #for ind in self.match_inds(spec=determined):
                    #    yield ind
        else:
            yield determined
        
def get_filter_from_message(message):
    """ Returns a list of indicator dimensions from a message
    containing SDMX data """
    dim_list = message["structure"]["dimensions"]["observation"]
    return Filter([d["id"] for d in dim_list])


    
def parse_attributes(message, most_common_attributes, abbreviate=True):
    """ Extract a dictionary of attributes for the recorded observations.
    Where only one value for each attribute is recorded, this goes in the metadata
    Where multiple values are recorded, find the most common value and
    record data points that deviate from this value.
    """
    attributes = message["structure"]["attributes"]["observation"]
    r = {}
    for attribute, most_common in zip(attributes, most_common_attributes):
        name = attribute["name"]
        values = attribute["values"]
        if len(values) == 1:
            value = values[0]
        else:
            # If there is more than one value, use the most common
            value = values[most_common]
        r[name] = {
                "description": attribute["description"],
                "value": value["name"]
                }
        if "description" in value:
            r[name]["value_description"] = value["description"]
    if abbreviate:
        return abbreviate_attributes(r)
    else:
        return r
    
def abbreviate_attributes(att_dict):
    r = {}
    for k, v in att_dict.items():
        new_key = "{} ({})".format(k, v["description"])
        if "value_description" in v:
            new_value = "{} ({})".format(v["value"], v["value_description"])
        else:
            new_value = v["value"]
        r[new_key] = new_value
    return r

def identify_most_common_attributes(message):
    observations = message["dataSets"][0]["observations"]
    attributes = message["structure"]["attributes"]["observation"]
    def gen():
        for i in range(len(attributes)):
            counter = Counter(obs[i+1] for obs in observations.values())
            yield counter.most_common(1)[0][0]
    
    return list(gen())

def sdmx_to_icy(message, include_metadata=True):
    """ Convert an SDMX message containing data to indicator, country, year 
    json format. Optionally, the format can contain metadata and notes. 
    TODO: check if there are cases when >1 dataset is returned! 
    TODO: add the full indicator metadata at the end of the message if meta=True
    """
    observations = message["dataSets"][0]["observations"]
    dimensions = message["structure"]["dimensions"]["observation"]
    attributes = message["structure"]["attributes"]["observation"]
    filt = Filter([d["id"] for d in dimensions])
    
    # Return an ind: {country: {year: value}} nested dictionary
    r = defaultdict(lambda: defaultdict(dict))
    indicator_metadata = {}
    
    # Identify the most common attributes
    most_common_attributes = identify_most_common_attributes(message)
    deviations = []
    for observation_key, observation_value in observations.items():
        # We convert a numberical key like 0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0
        # into a dictionary of dimensions and then back into a more readable
        # string like NERA.PT.M..., 
        dim_dict = {}
        dim_names_dict = {}
        for key_value, dimension in zip(observation_key.split(":"), dimensions):
            dimension_id = dimension["id"]
            value_dict = dimension["values"][int(key_value)]
            dim_dict[dimension_id] = value_dict["id"]
            dim_names_dict[dimension["name"]] = value_dict["name"]
        indicator_id = filt.dict_to_key(dim_dict)
        indicator_metadata[indicator_id] = dim_names_dict
        # TODO: remove the ref area and time period from the metadata
        ref_area = dim_dict["REF_AREA"]
        time_period = dim_dict["TIME_PERIOD"]
        r[indicator_id][ref_area][time_period] = observation_value[0]
        for attribute, observed_attribute, most_common in zip(attributes, observation_value[1:], most_common_attributes):
            if observed_attribute != most_common:
                value = attribute["values"][observed_attribute]
                deviation_info = {"indicator": indicator_id, 
                                   "country": ref_area, 
                                   "year": time_period, 
                                   attribute["name"]: value["name"]}
                if "description" in value:
                    deviation_info["value_description"] = value["description"]
                deviations.append(deviation_info)

    if include_metadata:
        attributes = parse_attributes(message, most_common_attributes)
        r["metadata"] = {
                "indicators": indicator_metadata,
                "attributes": attributes,
                "exceptions": deviations
                }
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