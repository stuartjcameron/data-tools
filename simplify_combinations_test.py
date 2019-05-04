# -*- coding: utf-8 -*-
"""
Created on Sun Apr 14 20:50:58 2019

@author: WB390262
"""
from collections import Counter
def remove_dimension(c, i):
    return [("" if i == j else d) for j, d in enumerate(c.split("."))]

def is_unique(code, all_codes, ignoring):
    """ Check if code is unique within all_codes - missing dimensions can be
    matched against any value. The dimension with index ignore_dimension
    will be ignored. """
    match = 0
    for comparison in all_codes:
        for i, (a, b) in enumerate(zip(code, comparison)):
            if i != ignoring and (a == b or a == ""): # or if b == ""?
                match += 1
                if match > 1:
                    return False
    if match != 0:
        raise ValueError("Problem with codes", match)
    return True
                    
def simplify_combos(codes, quiet=False):
    codes = [c.split(".") for c in codes]
    for d in reversed(range(5)):
        print("simplify_combos", d)
        codes = [(remove_dimension(c, d) if is_unique(c, codes, ignoring=d) else c)
            for c in codes]
    return codes


combos = [
        "a.z.x.y.z",
        "a.b.c.d.g",
        "a.b.c.e.h",
        "b.z.d.g.i",
        "b.z.d.h.j"]

print(simplify_combos(combos))