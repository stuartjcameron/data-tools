# -*- coding: utf-8 -*-
"""
Class wrapping a dictionary of labels for UIS indicators
@author: scameron
"""


from uis_spec import UISSpec
import logging as lg

lg.basicConfig(level=lg.DEBUG)

class UISInd(object):
    """ UIS indicator 
    
    Class for storing dimension-value pairs that fully specify a UIS
    indicator 
    Also wraps a dictionary of labels
    Can be passed to an API query to get all data that matches the specification
    - will only match one specific indicator (or none)
    
    """
    def __init__(self, sdmx_key=None, uis_name=None, short_key=None, index=None):
        if not index:
            index = self.get_index(sdmx_key, uis_name, short_key)
        self.key = self.keys[index]
        self.short_key = self.short_keys[index]
        self.uis_name = self.uis_names[index]
        self.spec = UISSpec.from_key(self.key)
    
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
        for k, v in self.spec.parameters.items():
            if k in spec and spec[k] not in [None, "", v]:
                return False
        return True
        
    @classmethod
    def from_short_key(cls, k):
        pass
        
    def short_key(self):
        pass
        
    
