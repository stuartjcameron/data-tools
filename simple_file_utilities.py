# -*- coding: utf-8 -*-
"""
Small helper functions for working with files

@author: scameron
"""

import json
import csv

def to_json(filename, obj):
    with open(filename + ".json", "w") as f:
        json.dump(obj, f)
        
        
def from_json(filename):
    with open(filename + ".json", "r") as f:
        return json.load(f)
    #TODO: option to load as dataframe
    
    
def from_textlist(filename):
    """ Read newline-separated text file into a list of strings. """
    with open(filename + ".txt", "r") as f:
        for line in f:
            yield line.strip()
            
            
def to_textlist(filename, it):
    """ Write a list of strings to text """
    with open(filename + ".txt", "w") as f:
        for line in it:
            f.write("{}\n".format(line))
    

def dicts_to_csv(filename, lod, fieldnames=None):
    """ Write a list of dicts to a CSV file """
    if not fieldnames:
        fieldnames = lod[0].keys()
    with open(filename + ".csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames, extrasaction="ignore")
        writer.writeheader()
        for d in lod:
            writer.writerow({k: s.encode('utf-8') for k, s in d.items()})
