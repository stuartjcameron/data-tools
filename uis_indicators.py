# -*- coding: utf-8 -*-
"""
Class wrapping a dictionary of labels for UIS indicators
@author: scameron
"""

from icy_sdmx import Filter

#from uis_api_wrapper import Spec
import logging as lg
#don't need this - just initialize the spec with the list of dimensions...

fltr = Filter(dimensions=[
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
            ])


lg.basicConfig(level=lg.DEBUG)

class UISInd(object):
    """ UIS indicator 
    
    Wraps a dictionary of indicator labels and specifications with convenient
    lookup functions
    
    """
    def __init__(self, sdmx_key=None, uis_name=None, short_key=None, index=None):
        if not index:
            index = self.get_index(sdmx_key, uis_name, short_key)
        self.key = self.keys[index]
        self.short_key = self.short_keys[index]
        self.uis_name = self.uis_names[index]
        self.spec = fltr.key_to_dict(self.key)
    
    @classmethod
    def get_index(cls, sdmx_key=None, uis_name=None, short_key=None):
        if sdmx_key:
            return cls.keys.index(sdmx_key)
        elif uis_name:
            return cls.uis_names.index(uis_name)
        elif short_key:
            return cls.short_keys.index(short_key)
        
    @classmethod
    def match_spec(cls, spec):
        """ List all indicators in the dictionary that fit a given specification """
        
        
    def matches_spec(self, spec):
        for k, v in self.spec.items():
            if k in spec and spec[k] not in [None, "", v]:
                return False
        return True
        
    @classmethod
    def from_short_key(cls, k):
        pass
        
    def short_key(self):
        pass
        
    
