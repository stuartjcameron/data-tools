# -*- coding: utf-8 -*-
"""
Some small helper functions/classes for working with files

@author: scameron
"""

import json
import csv
from os import path
from pandas import DataFrame as DF

class Cache(object):
    """ Simple class for saving objects to a folder as json, csv or other
    format.
    """
    def __init__(self, folder):
        self.folder = folder
        
    def to_json(self, filename, obj):
        with open(self.json_path(filename), "w") as f:
            json.dump(obj, f)
            
    def from_json(self, filename):
        with open(self.json_path(filename), "r") as f:
            return json.load(f)
        #TODO: option to load as dataframe
        
    def from_textlist(self, filename):
        with open(self.text_path(filename), "r") as f:
            for line in f:
                yield line.strip()
                
    def to_textlist(self, filename, it):
        with open(self.text_path(filename), "w") as f:
            for line in it:
                f.write("{}\n".format(line))
        
    def path(self, filename):
        """ Return a path within the cache for the given filename """
        return path.join(self.folder, filename)
    
    def text_path(self, filename):
        return path.join(self.folder, "{}.txt".format(filename))    
        
    def json_path(self, filename):
        return path.join(self.folder, "{}.json".format(filename))
    
    def csv_path(self, filename):
        return path.join(self.folder, "{}.csv".format(filename))
    
    def df_to_json(self, filename, df):
        df.to_json(self.json_path(filename), orient="index")   
            
    def df_to_csv(self, filename, df):
        df.to_csv(self.csv_path(filename))
        
    def df_from_json(self, filename):
        #TODO: check this works
        return DF.from_json(self.json_path(filename), orient="index")
        
    def dicts_to_csv(self, filename, lod, fieldnames=None):
        """ Write a list of dicts to a CSV file """
        if not fieldnames:
            fieldnames = lod[0].keys()
        with open(self.csv_path(filename), "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames, extrasaction="ignore")
            writer.writeheader()
            for d in lod:
                writer.writerow(d)
