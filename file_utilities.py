# -*- coding: utf-8 -*-
"""
Some small helper functions/classes for working with files

@author: scameron
"""

import json
import csv
from os import path
from pandas import DataFrame

class Cache(object):
    """ Simple class for saving objects to a folder as json, csv or other
    format.
    """
    def __init__(self, folder):
        self.folder = folder
        
    def save(self, filename, obj, csv_fieldnames=None, fmt="json"):
        if type(obj) == DataFrame:
            self.save_df(filename, obj, fmt)
        elif csv_fieldnames is not None or fmt == "csv":
            self.dicts_to_csv(filename, obj, csv_fieldnames)
        else:
            with open(self.file(filename, "json"), "w") as f:
                json.dump(obj, f)
            
    def load(self, filename):
        with open(self.file(filename, "json"), "r") as f:
            return json.load(f)
        #TODO: option to load as dataframe
        
    def file(self, filename, fmt=None):
        """ Return a path within the cache for the given filename and format """
        if fmt in ["json", "csv"]:
            filename = "{}.{}".format(filename, fmt)
        return path.join(self.folder, filename)
    
    def save_df(self, filename, df, fmt="json"):
        path = self.file(filename, fmt)
        if fmt == "csv":
            df.to_csv(path)
        else:
            df.to_json(path, orient="index")        
        
    def dicts_to_csv(self, filename, lod, fieldnames=None):
        """ Write a list of dicts to a CSV file """
        if not fieldnames:
            fieldnames = lod[0].keys()
        with open(self.file(filename, "csv"), "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames)
            writer.writeheader()
            for d in lod:
                writer.writerow(d)
