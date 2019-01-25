# -*- coding: utf-8 -*-
"""
A database of country names with variants used by different international
organisations. Includes a country class 

Data will be stored in a big JSON file containing:
    1. a dictionary with ISO2 codes as keys and a list of the other names for 
    each country
    2. metadata including the ordering of the other names, agency and type
    of each other name
     
Structure of the main database:
{"name-lists": [ {"name": "ISO3"}, {"name": "def-short", "full-name": "Default short names",
                "link": "..."}, {"name": "wb-short", "agency": "World Bank", "link": "..."}],
"names": {
"BE": ["BEL", "Belgium", "Kingdom of Belgium"],
}
    }

This module will provide functions for managing this database, including:
    a function to add a new name list, based on a CSV and metadata. This will
    edit the JSON. The input CSV does not need to be complete.
    
    a function that reads the main JSON database into memory and allows it
    to be accessed via Country and CountryNameList
    
    various functions to query this data and re-output in CSV or JSON.

See https://stackoverflow.com/questions/4060221/how-to-reliably-open-a-file-in-the-same-directory-as-a-python-script
to open the JSON file in the same directory

Then the JSON file will be stored on Git or somewhere so it can be edited
with a main 'official' version that is used by default...

Also consider: want to be able to use this as the beginning of a larger country metadatabase
e.g. if we wanted to store data on which GPE country group they belong to, or which WB income group. This would be 
better stored as a 'column' than as separate lists of countries.

Explicitly allow categories to be added but discourage actual data (e.g. GDP)! The DB will then allow multiple
ways of searching by category - i.e. list all countries in a category or tabulate with categories as a column.


Created on Wed Sep  5 22:56:43 2018

@author: scameron
"""
import json
import csv
import datetime
import os

# from https://stackoverflow.com/questions/4060221/how-to-reliably-open-a-file-in-the-same-directory-as-a-python-script
_location = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))


class CountryNameList(object):
    """
        Stores metadata on a country name list with functions to get the
        name list
    """
    instances = []
    def __init__(self, name, filename, name_type, full_name=None,
                 agency=None, link=None,
                 valid_from=None, valid_to=None):
        VALID_NAME_TYPES = ["code", "abbreviation", "short", "long"]
        VALID_AGENCIES = ["World Bank", "UNICEF", "UNESCO"]
        if name_type in VALID_NAME_TYPES:
            self.name_type = name_type
        else:
            raise ValueError("Name type must be one of: {}".format(", ".join(VALID_NAME_TYPES)))
        if agency is None or agency in VALID_AGENCIES:
            self.agency = agency
        else:
            raise ValueError("Agency must be one of: {}".format(", ".join(VALID_AGENCIES)))
        self.name = name
        self.full_name = full_name
        self.link = link
        self.valid_from = valid_from  # dates for which this country list is valid
        self.valid_to = valid_to
        self.added = datetime.datetime.now()
        self._get_data_from_csv(filename)
        self.instances.append(self)
        
    def _get_data_from_csv(self, filename):
        """ Get new country list from a CSV, store it and save the database """
        with open(filename, "r") as f:
            pass
        
        Country._save()
    

class Country(object):
    """
        Each country has ISO 2 and 3-letter codes and numeric code
        A default short and long name (which standard to use for these)?
        'names' - contains all abbreviations, short and long names
        'abbreviations' - contains all abbreviated names (e.g. PNG)
        'short_names' - contains all short names (e.g. PR Tanzania?)
        'long_names' - contains all long names (e.g. People's Republic...)
        'notes' - any names e.g. on changes to country over time
        
        Also attach the 2-letter and 3-letter codes to the class e.g.
        Country.BE will return Belgium
    
    """
    instances = []
    
    def __init__(self, name_dict):
        pass
    
    def has_word(self, w):
        """ Returns true if any of the country's names contain w (case-insensitive) """
        w = w.lower().strip()
        for name in self.names:
            if w in name.lower():
                return True
        return False
    
    def has_name(self, s):
        """ Whether s matches one of the country's names. Case and space insensitive """
        s = s.lower().strip()
        for name in self.names:
            if s == name.lower():
                return True
        return False
        
    def __eq__(self, item):
        """ Returns true if item is the same country or a string that matches one
        of this country's names """
        if type(item) == type(self):
            return self is item
        
        return self.has_name(item)
            
    @classmethod
    def filt(cls, func):
        return filter(func, cls.instances)
    
    @classmethod
    def first(cls, func):
        for country in cls.instances:
            if func(country):
                return country
        
    @classmethod
    def with_word(cls, w):
        """ Return the first country object that contains w """
        return cls.first(lambda c: c.has_word(w.lower()))
        
    @classmethod
    def lookup(cls, item):
        """ Return the country object equal to item """
        return cls.first(lambda c: c == item)
        
    @classmethod
    def _save(cls, filename=None):
        """ Save the whole database including metadata """
        if filename == None:
            filename = os.path.join(_location, "countries.json")
        with open(filename, "w") as f:
            pass

    @classmethod
    def load(cls, filename=None):
        """ Load all country data from the file """
        if filename == None:
            filename = os.path.join(_location, "countries.json")
        with open(filename, "r") as f:
            pass
            # first get the name list info, then the country info...
            
Country.load()
    
class CountryGroup(object):
    def __init__(self, list_of_countries):
        pass


                
        

