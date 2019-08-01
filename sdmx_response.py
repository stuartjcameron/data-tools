# -*- coding: utf-8 -*-
"""
Class for conveniently extracting information from an SDMX request response object
and raising helpful error messages when the information isn't there.

@author: https://github.com/stuartjcameron
"""
from collections import defaultdict, Counter
from const_flag import Flag

def endless_defaultdict():
    return defaultdict(endless_defaultdict)


def defaultdict_to_dict(d):
    if isinstance(d, dict):
        return {k: defaultdict_to_dict(v) for k, v in d.items()}
    else:
        return d



def key_to_list(key):
    """ Convert an SDMX numerical key like 0:0:0... to a list of integers """
    return [int(part) for part in key.split(":")]
    

class cached_property(object):
    """
    A property that is only computed once per instance and then replaces itself
    with an ordinary attribute. Deleting the attribute resets the property.
    
    Simplified from following source
    Source: https://github.com/bottlepy/bottle/commit/fa7733e075da0d790d809aa3d2f53071897e6f76
    """  
    
    def __init__(self, func):
        self.__doc__ = getattr(func, "__doc__")
        self.func = func

    def __get__(self, obj, cls):
        if obj is None:
            return self
        value = obj.__dict__[self.func.__name__] = self.func(obj)
        return value
    
    
class cached_gen(object):
    """
    A property based on a generator function that is only computed once per 
    instance and then replaces itself
    with an ordinary attribute. Deleting the attribute resets the property.
    
    Simplified from following source
    Source: https://github.com/bottlepy/bottle/commit/fa7733e075da0d790d809aa3d2f53071897e6f76
    """  

    def __init__(self, func):
        self.__doc__ = getattr(func, "__doc__")
        self.func = func

    def __get__(self, obj, cls):
        if obj is None:
            return self
        value = obj.__dict__[self.func.__name__] = list(self.func(obj))
        return value

    
class NoDataException(Exception):
    pass

METADATA = Flag("Metadata", "DIMENSIONS ATTRIBUTES ATTRIBUTE_DESCRIPTIONS EXCEPTIONS")

  
class SdmxCsvResponse(object):
    #TODO: write this
    def __init__(self, response):
        self.response = response
        self.message = response.text
        
    @cached_property
    def data(self):
        from io import StringIO
        import csv
        reader = csv.reader(StringIO(self.response.text))
        return list(list(row) for row in reader)
        
    @cached_property
    def dataframe(self):
        import pandas as pd
        from io import StringIO
        return pd.read_csv(StringIO(self.response.text))
    
    def save(self, file):
        with open(file, "w") as f:
            f.write(self.response.text)

class SdmxJsonResponse(object):
    def __init__(self, response):
        self.response = response
        self.message = response.json()
        self.structured = False
        
    @cached_property
    def data(self):
        try:
            r = self.message["dataSets"][0]["observations"]
            list(r)[0]   # Make sure it contains at least one key-value
            return r
        except (KeyError, IndexError):
            raise NoDataException("No observations found in SDMX response"
                                  + self.response_text)
            # TODO: add the URL queried and the content of the message
        
    @cached_property
    def attributes(self):
        try:
            return self.message["structure"]["attributes"]["observation"]
        except KeyError:
            raise NoDataException("No attributes found in SDMX response"
                                  + self.response_text)
    
    @cached_property
    def dimensions(self):
        try:
            return self.message["structure"]["dimensions"]["observation"]
        except KeyError:
            raise NoDataException("No dimensions found in SDMX response"
                                  + self.response_text)
            # TODO: add the URL queried and the content of the message
    
    @cached_property
    def response_info(self):
        r = self.response
        return {
                "URL": r.request.url,
                "response": r.text
                }               
        
    @cached_property
    def response_text(self):
        return "\n" + "\n".join("{}: {}".format(k, v) 
                                for k, v in self.response_info.items())
        
    def set_structure(self, ref_area="REF_AREA", time_period="TIME_PERIOD"):
        """ Set a structure such that all dimensions other than 
        ref_area and time_period will be treated as aspects of an indicator. """
        self.ref_area = ref_area
        self.time_period = time_period
        self.structured = True
        self.indicator_dimensions = [d["id"] for d in self.dimensions
                                     if not d["id"] in [self.ref_area, self.time_period]]
        
    @cached_gen
    def most_common_attribute_numbers(self):
        """ Returns the most common attribute indices for 
        each attribute provided in the observations. """
        for i in range(len(self.attributes)):
            counter = Counter(obs[i+1] for obs in self.data.values())
            yield counter.most_common(1)[0][0]

    
    @cached_property
    def most_common_attributes(self):
        """ Dictionary of the most common values of each attribute """
        return {attribute["name"]: attribute["values"][most_common]["name"]
                for attribute, most_common
                in zip(self.attributes, self.most_common_attribute_numbers)}


    @cached_property
    def attribute_descriptions(self):
        """ Dictionary of the descriptions of each attribute. Also includes
        descriptions of the most common value of each attribute, if present. """
        r = {}
        for attribute, most_common in zip(self.attributes, self.most_common_attribute_numbers):
            name = attribute["name"]
            value = attribute["values"][most_common]
            r[name] = attribute["description"]
            if "description" in value:
                key = "{}: {}".format(name, value["name"])
                r[key] = value["description"]
        return r
    
    
    @cached_property
    def dataframe(self):
        """ Convert an SDMX response containing data to a pandas dataframe
            The dataframe will be structured in long format:
            ref_area | time_period | indicator | value in columns
            
            Renames headings using the given function
        """
        import pandas as pd
        dimension_ids = []
        attribute_ids = []
        maps = {}
        for d in self.dimensions:
            maps[d["id"]] = pd.Series(v["id"] for v in d["values"])
            dimension_ids.append(d["id"])
        for a in self.attributes:
            maps[a["id"]] = pd.Series(v["name"] for v in a["values"])
            attribute_ids.append(a["id"])
        columns = dimension_ids + ["Value as string"] + attribute_ids
        data = (key_to_list(k) + v for k, v in self.data.items())
        r = pd.DataFrame(data=data, columns=columns)
        r["Value"] = pd.to_numeric(r["Value as string"])
        
        # Replace dimension and attribute numbers with the relevant ID / name
        for column, series in maps.items():
            r[column] = r[column].map(series)
        
        r["Key"] = r[dimension_ids].apply(".".join, axis=1)
        if self.structured:
            r["Indicator key"] = r[self.indicator_dimensions].apply(".".join, axis=1)
        return r
    
    
    @cached_property
    def indicator_metadata(self):
        """ Return names and descriptions of each of the dimensions """
        r = {}
        if self.structured:
            use = lambda d: d["id"] in self.indicator_dimensions
        else:
            use = lambda d: True
        
        for observation_key in self.data.keys():
            zipped = list(zip(self.dimensions, key_to_list(observation_key)))
            key_string = ".".join(d["values"][v]["id"] for d, v in zipped if use(d))
            if not key_string in r:
                r[key_string] = {d["name"]: d["values"][v]["name"] for d, v in zipped if use(d)}
        return r
    
    
    def parse_key(self, numerical_key):
        """ Take a numerical key like 0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0
         and look up the relevant dimension information.
         
         If a structure has been set, this will return a tuple of
         valid SDMX key for the indicator (NERA.PT.M..., ), ref_area (country), and time_period (year)
         
         If not it will return the SDMX key only
         """
        if self.structured:
            indicator = []
            for d, v in zip(self.dimensions, key_to_list(numerical_key)):
                value_id = d["values"][v]["id"]
                if d["id"] == self.ref_area:
                    ref_area = value_id
                elif d["id"] == self.time_period:
                    time_period = value_id
                else:
                    indicator.append(value_id)
            return ".".join(indicator), ref_area, time_period
            
        else:
            return ".".join(d["values"][v]["id"] for d, v in 
                            zip(self.dimensions, key_to_list(numerical_key)))


    def get_nested(self, metadata=METADATA.ALL):
        """ Convert an SDMX response containing data to a hierarchical 
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
           
        """
        # Return an ind: {country: {year: value}} nested dictionary
        r = defaultdict(lambda: defaultdict(dict))
        for k, v in self.data.items():
            if self.structured:
                i, c, y = self.parse_key(k)
                r[i][c][y] = v[0]
            else:
                r[self.parse_key(k)] = v[0]
                
        if metadata:
            r["metadata"] = self.get_metadata(metadata)
        return r
    
    
    @cached_property
    def deviations(self):
        """ 
        A dictionary of observations that have attributes deviating from
        the most common values. 
        """
        r = endless_defaultdict()
        for observation_key, observation_value in self.data.items():
            if self.structured:
                i, c, y = self.parse_key(observation_key)
                d = lambda: r[i][c][y]
            else:
                d = lambda: r[self.parse_key(observation_key)]
            z = zip(self.attributes, 
                    observation_value[1:], 
                    self.most_common_attribute_numbers)
            for attribute, observed_attribute, most_common in z:
                if observed_attribute != most_common:
                    value = attribute["values"][observed_attribute]
                    d()[attribute["name"]] = value["name"]
                    if "description" in value:
                        key = "{} description".format(attribute["name"])
                        d()[key] = value["description"]
        return defaultdict_to_dict(r)
    
    
    def get_metadata(self, include=METADATA.ALL):
        r = {}
        if include.DIMENSIONS:
            r["indicators"] = self.indicator_metadata
        if include.ATTRIBUTES:
            r["attributes"] = self.most_common_attributes
        if include.ATTRIBUTE_DESCRIPTIONS:
            r["attribute_descriptions"] = self.attribute_descriptions
        if include.EXCEPTIONS:
            r["exceptions"] = self.deviations
        return r


def nested_to_tuples(top):
    """ Generator that flattens nested dictionaries in the 
        indicator: {country: {year: value}} format to 
        (indicator, country, year, value) tuples
    """    
    for indicator, middle in top.items():
        for country, bottom in middle.items():
            for year, value in bottom.items():
                yield indicator, country, year, value


def nested_to_df(nested):
    """ Convert nested dictionaries in the indicator: {country: {year: value}}
        format to a vertical format pandas dataframe
    """
    import pandas as pd
    columns = ["Indicator", "Country", "Year", "Value as string"]
    r = pd.DataFrame.from_records(nested_to_tuples(nested), columns=columns)
    r["Value"] = pd.to_numeric(r["Value as string"])
    return r

