# -*- coding: utf-8 -*-
"""
Request data from an SDMX API 

This has been designed for use with the UNESCO Institute of Statistics API.
It is easiest to use the `uis` module which wraps sdmx_api and sdmx_response
with more convenient methods.

I hope it will work with other SDMX APIs too but it is not yet tested.
    
@author: https://github.com/stuartjcameron
"""

import inflection
import requests
import logging as lg
from urllib.parse import urljoin
from collections import defaultdict
from sdmx_response import SdmxResponse


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
    
    #def is_complete(self, d):
    #    """ Test whether a dictionary represents a complete indicator """
    #    return all(d[k] for k in self.dims())
    # no longer used
    
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
        """
        Returns an SDMX key for the set of dimensions represented
        by the dictionary d. If ind=False it may also return ref_area
        and time_period (if these are in the dict)
        """ 
        if d:
            return ".".join(value_to_filter_string(d.get(k))
                        for k in self.dims(ind))
        else: # d not specified, None or {} -- empty query
            return "." * (len(self.dims(ind)) - 1)
            
    def key_to_dict(self, key, ind=True):
        return dict(zip(self.dims(ind), key.split(".")))
        
    def key_to_dict_old(self, key, ind=True):
        """ Returns a dict based on an SDMX key """
        return self.extract_dims(dict((zip(self.all_dims, key.split(".")))))
    
def combine_queries(*query_dicts):
    """
    Combine dicts representing indicators or queries into a 
    single dict with multiple values.
    Note this can 'over-query', i.e. return too many results if there
    is too much difference between the queries. 
    """
    r = defaultdict(set)
    for d in query_dicts:
        for key, value in d.items():
            if type(value) is list:
                r[key] |= set(value)
            else:
                r[key].add(value)
    return {k: list(v) for k, v in r.items()}
 
def value_to_filter_string(v):
    """ 
    Convert None, a number, string or list into a string for 
    inclusion in an SDMX filter string
    """
    if v is None:
        return ""
    elif type(v) == list:
        return "+".join(v)
    else:
        return str(v)

def camel(k):
    """ 
    Appropriately camelize a keyword argument key for inclusion in 
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
        self.process_response = SdmxResponse

        
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
        response = requests.get(url, params=params, verify=self.verification)
        return self.process_response(response)
    
    def query(self, **kwargs):
        """ Convenience function for querying the API. Accepts any parameter
        that can filter the query, and passes the remainder as parameters to 
        the API request.
        """
        spec, remainder = self.filter.extract_dims_and_remainder(kwargs, False)
        return self.get(spec, remainder)
        
        
    def get_dimension_information(self, spec=None):
        """ Get the dimension information from the API for the given spec """
        params = {"detail": "serieskeysonly", 
                 "dimension_at_observation": "AllDimensions"}
        response = self.get(spec, params)
        return response.dimensions
    
        
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
        
