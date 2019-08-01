# -*- coding: utf-8 -*-
"""
Miscellaneous string utilities to help the UIS API module

Created on Wed Jul 31 10:51:35 2019

@author: https://github.com/stuartjcameron
"""
import re
import string

UNPUNCTUATED = str.maketrans(string.punctuation, ' '*len(string.punctuation))
# Regexes (can be improved but good enough!)
NOT_SNAKE = re.compile(r"[^a-z0-9_]")  
NOT_UPPER_SNAKE = re.compile(r"[^A-Z0-9_]")
NOT_CAMEL = re.compile(r"[^a-zA-Z0-9]")


def is_snake(s):
    """ Lower case, contains underscore and only contains a-z, 0-9 and _ """
    return "_" in s and not NOT_SNAKE.search(s) 


def is_upper_snake(s):
    """ Upper case, contains underscore and only contains A-Z, 0-9 and _ """
    return "_" in s and not NOT_UPPER_SNAKE.search(s)


def is_camel_case(s):
    """ Mixed case and only contains a-z, A-Z, 0-9  """
    return not(s.islower() or s.isupper() or NOT_CAMEL.search(s))

        
def header_case(s):
    """ Convert string to a nice looking format for a heading.
    Convert snake_case, CAPS_SNAKE_CASE, Title Case, all lower, all upper,
    and CamelCase to Sentence case
    Other mixed strings (e.g. "UN country name") will be left as is, to preserve
    acronyms
    """
    if is_snake(s) or is_upper_snake(s):
        return s.replace("_", " ").capitalize()
    elif is_camel_case(s):
        r = re.sub(r"[A-Z]", lambda matched: " " + matched.group(0), s)
        return r.strip().capitalize()
    elif s.istitle() or s.islower() or s.isupper():
        return s.capitalize()
    return s


def clean_label(s):
    """ Clean up a label or heading for easy matching.
        Convert punctuation to spaces, convert to lower case, 
        remove multiple spaces, and strip whitespace at the beginning or end.
        "This!!!is3 a  crazy HEADING..." -> "this is3 a crazy heading"
        "STAT_UNIT", "Stat. Unit" -> "stat unit"
    """
    s = s.translate(UNPUNCTUATED).lower()
    return re.sub(' +', ' ', s).strip()

# not used   
#def clean_heading(s):
#    return re.sub("[^a-z0-9]", " ", s.lower())


def camel(k):
    """ 
    Appropriately camelize a keyword argument key for inclusion in 
    an SDMX URL query 
    e.g. start_period => startPeriod 
    
    (Taken from inflection package)
    """
    return k[0].lower() + re.sub(r"(?:^|_)(.)", lambda m: m.group(1).upper(), k)[1:]
    #return inflection.camelize(k, uppercase_first_letter=False)