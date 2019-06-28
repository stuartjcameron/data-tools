# -*- coding: utf-8 -*-
"""
Overkill class for creating sets of binary flags to pass to functions

Example usage:
   
ANIMALS = Flag(["cat", "dog", "cow"])
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

@author: WB390262
"""

class ConstFlag():
    def __init__(self, attributes):
        self.attributes = [a.upper() for a in attributes]
        for attr in self.attributes:
            setattr(self, attr, Attribute(self, [attr]))
        self.ALL = Attribute(self, self.attributes)
        self.NONE = Attribute(self, [])
        
class Attribute():
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
            return Attribute(self.parent, list(set(self._attributes + other._attributes)))
        else:
            raise TypeError("Cannot combine flags of different types")
            
    def __bool__(self):
        return any(getattr(self, a) for a in self.parent.attributes)
    
    def __contains__(self, item):
        return all(getattr(self, a) for a in item._attributes)
        #TODO - allow string contains too
    
    