# -*- coding: utf-8 -*-
"""
Convert an SDMX message into convenient formats including
'indicator-country-year' - a JSON object e.g.
    {indicator1: {BD: {2012: 15, 2015: 22}}, {UG: {2012: 44, 2016: 79}}}

    
@author: scameron
"""    
import logging as lg
from collections import defaultdict, Counter
import pandas as pd
from numpy import NaN

def endless_defaultdict():
    return defaultdict(endless_defaultdict)



lg.basicConfig(level=lg.DEBUG)
LONG = object()
WIDE = object()

class METADATA(object):
    """ Constants for indicating which metadata to return """
    DIMENSIONS = 1  
    ATTRIBUTES = 2
    ATTRIBUTE_DESCRIPTIONS = 4
    EXCEPTIONS = 8
    ALL = 2**11 - 1

  
def parse_attributes(message, most_common_attributes):
    """ Extract a dictionary of attributes for the recorded observations.
    Where only one value for each attribute is recorded, this goes in the metadata
    Where multiple values are recorded, find the most common value and
    record data points that deviate from this value.
    """
    attributes = message["structure"]["attributes"]["observation"]
    attribute_dict = {}
    attribute_description_dict = {}
    for attribute, most_common in zip(attributes, most_common_attributes):
        name = attribute["name"]
        values = attribute["values"]
        if len(values) == 1:
            value = values[0]
        else:
            # If there is more than one value, use the most common
            value = values[most_common]
        attribute_dict[name] = value["name"]
        attribute_description_dict[name] = attribute["description"]
        if "description" in value:
            attribute_description_dict["{}: {}".format(name, value["name"])] = value["description"]
    return attribute_dict, attribute_description_dict
   

def identify_most_common_attributes(message):
    observations = message["dataSets"][0]["observations"]
    attributes = message["structure"]["attributes"]["observation"]
    def gen():
        for i in range(len(attributes)):
            counter = Counter(obs[i+1] for obs in observations.values())
            yield counter.most_common(1)[0][0]
    
    return list(gen())


def to_icy(message, metadata=METADATA.ALL, middle="REF_AREA", bottom="TIME_PERIOD"):
    """ Convert an SDMX message containing data to a hierarchical json
    format with indicators, countries (ref_area), and years (time_period)
    as keys in nested dictionaries.
    
    By default, all metadata and notes are included. Use
    metadata
        =None to omit metadata
        =METADATA.DIMENSIONS to include the dimensions only
        =METADATA.ATTRIIBUTES to include attributes only
        =METADATA.ATTRIBUTES | METADATA.ATTRIBUTE_DIMENSIONS to include descriptions
        of the attributes
        =METADATA.EXCEPTIONS to include information about exceptional attributes
        (e.g. not applicable values).
        
    TODO: currently descriptions are only provided for the main value
    of each description - provide for all instead.        
    """
    observations = message["dataSets"][0]["observations"]
    dimensions = message["structure"]["dimensions"]["observation"]
    attributes = message["structure"]["attributes"]["observation"]
    
    # Return an ind: {country: {year: value}} nested dictionary
    r = defaultdict(lambda: defaultdict(dict))
    indicator_metadata = {}
    
    # Identify the most common attributes
    most_common_attributes = identify_most_common_attributes(message)
    deviations = endless_defaultdict()
    for observation_key, observation_value in observations.items():
        # We convert a numerical key like 0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0
        # into a dictionary of dimensions and then back into a more readable
        # string like NERA.PT.M..., 
        indicator_dims = []
        dim_names_dict = {}
        for key_value, dimension in zip(observation_key.split(":"), dimensions):
            dimension_id = dimension["id"]
            value_dict = dimension["values"][int(key_value)]
            value = value_dict["id"]
            if dimension_id == middle:
                ref_area = value
            elif dimension_id == bottom:
                time_period = value
            else:
                indicator_dims.append(value)
            #dim_dict[dimension_id] = value_dict["id"]
            dim_names_dict[dimension["name"]] = value_dict["name"]
            
        indicator_id = ".".join(indicator_dims)
        indicator_metadata[indicator_id] = dim_names_dict
        # TODO: remove the ref area and time period from the metadata
        #ref_area = dim_dict["REF_AREA"]
        #time_period = dim_dict["TIME_PERIOD"]
        r[indicator_id][ref_area][time_period] = observation_value[0]
        for attribute, observed_attribute, most_common in zip(attributes, observation_value[1:], most_common_attributes):
            if observed_attribute != most_common:
                value = attribute["values"][observed_attribute]
                deviation = deviations[indicator_id][ref_area][time_period]
                deviation[attribute["name"]] = value["name"]
                #deviation_info = {"indicator": indicator_id, 
                #                   "country": ref_area, 
                #                   "year": time_period, 
                #                   attribute["name"]: value["name"]}
                if "description" in value:
                    deviation["{} description".format(attribute["name"])] = value["description"]
                #deviations.append(deviation_info)

    if metadata:
        r["metadata"] = {}        
        if metadata & METADATA.DIMENSIONS:
            r["metadata"]["indicators"] = indicator_metadata
        if metadata & METADATA.ATTRIBUTES:
            attributes, attribute_descriptions = parse_attributes(message, most_common_attributes)
            r["metadata"]["attributes"] = attributes
            if metadata & METADATA.ATTRIBUTE_DESCRIPTIONS:
                r["metadata"]["attribute_descriptions"] = attribute_descriptions
        if metadata & METADATA.EXCEPTIONS:
            r["metadata"]["exceptions"] = deviations
    return r


def key_to_list(key):
    """ Convert an SDMX key like 0:0:0... to a list of integers """
    return [int(part) for part in key.split(":")]


def to_df(message):
    """ Convert a UIS SDMX message containing data to a pandas dataframe
        The dataframe will be structured in long format:
        ref_area | time_period | indicator | value in columns
        """
    observations = message["dataSets"][0]["observations"]
    dimensions = message["structure"]["dimensions"]["observation"]
    attributes = message["structure"]["attributes"]["observation"]
    dimension_ids = []
    attribute_ids = []
    maps = {}
    for d in dimensions:
        maps[d["id"]] = pd.Series(v["id"] for v in d["values"])
        dimension_ids.append(d["id"])
    for a in attributes:
        maps[a["id"]] = pd.Series(v["name"] for v in a["values"])
        attribute_ids.append(a["id"])
    columns = dimension_ids + ["Value as string"] + attribute_ids
    data = (key_to_list(k) + v for k, v in observations.items())
    df = pd.DataFrame(data=data, columns=columns)
    df["Value"] = pd.to_numeric(df["Value as string"])
    
    # Replace dimension and attribute numbers with the relevant ID / name
    for column, series in maps.items():
        df[column] = df[column].map(series)
    
    indicator_dimensions = [d for d in dimension_ids if d not in ["REF_AREA", "TIME_PERIOD"]]
    df["Key"] = df[dimension_ids].apply(".".join, axis=1)
    df["Indicator key"] = df[indicator_dimensions].apply(".".join, axis=1)
    return df
     

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