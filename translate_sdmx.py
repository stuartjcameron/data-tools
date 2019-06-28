# -*- coding: utf-8 -*-
"""
NOT USED - DELETE!

Convert an SDMX response into convenient formats including
'indicator-country-year' - a JSON object e.g.
    {indicator1: {BD: {2012: 15, 2015: 22}}, {UG: {2012: 44, 2016: 79}}}

Note: consider integrating these into the sdmx_response class
as they all work on the response object.
    
@author: scameron
"""    
import logging as lg
from collections import defaultdict
from const_flag import ConstFlag



lg.basicConfig(level=lg.DEBUG)
LONG = object()
WIDE = object()




def to_icy_OLD(response, metadata=METADATA.ALL, middle="REF_AREA", bottom="TIME_PERIOD"):
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
    dimensions = response.dimensions
    attributes = response.attributes
    
    # Return an ind: {country: {year: value}} nested dictionary
    r = defaultdict(lambda: defaultdict(dict))
    indicator_metadata = {}
    
    # Identify the most common attributes
    most_common_attributes = identify_most_common_attributes(response)
    deviations = endless_defaultdict()
    for observation_key, observation_value in response.data.items():
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
        # TODO: refactor! This will often rewrite the same indicator
        # metadata several times...
        # TODO: remove the ref area and time period from the metadata
        r[indicator_id][ref_area][time_period] = observation_value[0]
        for attribute, observed_attribute, most_common in zip(attributes, observation_value[1:], most_common_attributes):
            if observed_attribute != most_common:
                value = attribute["values"][observed_attribute]
                deviation = deviations[indicator_id][ref_area][time_period]
                deviation[attribute["name"]] = value["name"]
                if "description" in value:
                    key = "{} description".format(attribute["name"])
                    deviation[key] = value["description"]
                #deviations.append(deviation_info)

    if metadata:
        r["metadata"] = {}        
        if metadata.DIMENSIONS:
            r["metadata"]["indicators"] = indicator_metadata
        if metadata.ATTRIBUTES:
            attributes, attribute_descriptions = parse_attributes(response, most_common_attributes)
            r["metadata"]["attributes"] = attributes
            if metadata & METADATA.ATTRIBUTE_DESCRIPTIONS:
                r["metadata"]["attribute_descriptions"] = attribute_descriptions
        if metadata & METADATA.EXCEPTIONS:
            r["metadata"]["exceptions"] = deviations
    return r
    
    

     

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