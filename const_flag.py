# -*- coding: utf-8 -*-
"""
Overkill class for creating sets of binary flags to pass to functions
Pretty much duplicates enum.Flag but has useful .ALL and .NONE settings

Example usage:
   
ANIMALS = Flag("Animals", ["cat", "dog", "cow"])
or ANIMALS = Flag("Animals", "CAT DOG COW")
pets = ANIMALS.DOG | ANIMALS.CAT
pets.DOG  # True
pets.CAT  # True
pets.COW  # False
pets.GOAT # Error
ANIMALS.CAT in pets   # True
ANIMALS.GOAT in pets    # Error

def animal_noise(animals=ANIMALS.NONE):
    if animals.CAT:
        print("miaow")
    if animals.DOG:
        print("woof")
    if animals.COW:
        print("moo")
        
animal_noise()   # does nothing (object with flags all false)
animal_noise(pets)   # miaow, woof


Created on Fri Jun 21 16:18:06 2019

@author: https://github.com/stuartjcameron
"""

class Flag():
    """
    Enum-like flag
    """
    def __init__(self, typename, names):
        self.typename = typename
        if isinstance(names, str):
            self.attributes = names.split()            
        else:
            self.attributes = [a.upper() for a in names]
        for attr in self.attributes:
            setattr(self, attr, FlagAttributes(self, [attr]))
        self.ALL = FlagAttributes(self, self.attributes)
        self.NONE = FlagAttributes(self, [])

    def __repr__(self):
        return "<Flag {}>".format(self.typename)
        
class FlagAttributes():
    """
    List of named attributes associated with a Flag
    """
    def __init__(self, parent, attributes):
        self._attributes = [a.upper() for a in attributes]
        self.parent = parent
        not_found_attributes = set(self._attributes) - set(parent.attributes)
        if not_found_attributes:
            raise KeyError("Attribute(s) not available: {}".format(" ".join(not_found_attributes)))
        for attr in parent.attributes:
            setattr(self, attr, attr in self._attributes)
        
    def __or__(self, other):
        if self.parent == other.parent:
            return FlagAttributes(self.parent, list(set(self._attributes + other._attributes)))
        else:
            raise TypeError("Cannot combine flags of different types")
            
    def __bool__(self):
        return any(getattr(self, a) for a in self.parent.attributes)
    
    def __contains__(self, item):
        return all(getattr(self, a) for a in item._attributes)
        #TODO - allow string contains too
        
    def __repr__(self):
        return "<{}.{}>".format(self.parent.typename, "|".join(self._attributes))
    
    