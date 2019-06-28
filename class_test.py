# -*- coding: utf-8 -*-
"""
Created on Thu Jun 20 16:42:52 2019

@author: WB390262
"""

class cached_property(object):
    """
    A property that is only computed once per instance and then replaces itself
    with an ordinary attribute. Deleting the attribute resets the property.
    
    Simplified from following source
    Source: https://github.com/bottlepy/bottle/commit/fa7733e075da0d790d809aa3d2f53071897e6f76
    """  # noqa

    def __init__(self, func):
        self.__doc__ = getattr(func, "__doc__")
        self.func = func

    def __get__(self, obj, cls):
        if obj is None:
            return self
        value = obj.__dict__[self.func.__name__] = self.func(obj)
        return value

class Test(object):
    
    def __init__(self):
        self._x = 10
    
    @cached_property
    def x(self):
        self._x += 10
        return self._x
    
"""

Flag object...
e.g. 
METADATA = Flag(["dimensions", "exceptions", "attributes"])
x=METADATA.DIMENSIONS 
x.DIMENSIONS --> True
x.EXCEPTIONS --> False 
x.BLAH --> error

METADATA.DIMENSIONS | METADATA.EXCEPTIONS -->
    returns a new object x such that x.DIMENSIONS is True and x.EXCEPTIONS is True

x = METADATA.ALL --> x.DIMENSIONS, x.EXCEPTIONS, x.ATTRIBUTES are all True

"""
    
class Flag():
    def __init__(self, attributes):
        self.attributes = [a.upper() for a in attributes]
        for attr in self.attributes:
            setattr(self, attr, Attribute(self, [attr]))
        self.ALL = Attribute(self, self.attributes)
        self.NONE = Attribute(self, [])
        
class Attribute():
    def __init__(self, parent, attributes):
        self.__attributes__ = [a.upper() for a in attributes]
        self.parent = parent
        not_found_attributes = set(self.on_attributes) - set(parent.attributes)
        if not_found_attributes:
            raise KeyError("Attribute(s) not available: {}".format(" ".join(not_found_attributes)))
        for attr in parent.attributes:
            setattr(self, attr, attr in self.on_attributes)
        
    def __or__(self, other):
        if self.parent == other.parent:
            return Attribute(self.parent, list(set(self.on_attributes + other.on_attributes)))
        else:
            raise TypeError("Cannot combine flags of different types")


METADATA = Flag(["dimensions", "exceptions", "attributes"])
x=METADATA.DIMENSIONS 
y = METADATA.DIMENSIONS | METADATA.EXCEPTIONS 