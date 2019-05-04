# -*- coding: utf-8 -*-
"""
Test scripts for grabbing data from unesco website
Created on Wed Sep  5 22:56:43 2018

This uses the country module from the hdx library.
Note that for many visualizations or tables we would want a set of country
names that includes shorter forms. e.g.
hdx has
Micronesia (Federated States of) and Micronesia, Federated States of
with abbreviation 'Micronesia, Fed. Sts.'
and the reliefweb short name 'Micronesia'

we would want to add 'Micronesia, FS' or 'FS Micronesia' and make this the
standard form for visualizations.

Consider adding a class for getting data from a set of countries?
e.g. gpe_countries.iso2 would return a list of the iso2 codes

Ultimately consider a multi-agency tool that could pull data from multiple sources
with fuzzy queries that could grab both metadata (years, countries available) and
the actual data.

e.g. indicator "UIS>Adjusted net enrolment rate, primary school" could be found 
with 'primary ANER'
pull_data("primary ANER") would grab all the ANER data (by country and year, in a standard
format), knowing to look in UIS first for this.

This can then be used as the back end for a multi-agency data site and an online API
e.g. www.countrydata.dev/primary+aner would return the json series in the same standard
format.

The return series would also return a 'stable query' for grabbing this data,
noting that fuzzy requests could sometimes return a different data set in
future when new data sets are added.

To make this feasible it would be limited to data that is at the country level 
(but with some standard disaggregations allowed, e.g. for HHS based data)
and released up to once a year.

Using stats.uis to get some sample queries.

ANER and ANER (female) for Afghanistan and Argentina, 2012-2018
http://data.uis.unesco.org/RestSDMX/sdmx.ashx/GetData/EDULIT_DS/NERA_1_CP+NERA_1_F_CP.AFG+ARG/all?startTime=2012&endTime=2018

Is this completely different from the real data flows?! Different address. May be easier
to use this one, however!

Data structure definition URL: http://data.uis.unesco.org/RestSDMX/sdmx.ashx/GetDataStructure/EDULIT_DS
(what is this?!)


@author: scameron
"""

from hdx.location.country import Country
import inflection
import requests
from urllib.parse import urljoin
from string import Formatter
from os import path
import json
import csv

import pandas as pd
from collections import defaultdict, Counter
from itertools import combinations
import fnmatch
#working_dir = "C:/Users/scameron/Dropbox (Personal)/Py"
#with open(path.join(working_dir, "country_2letter_codes.json"), "r") as f:
#    all_codes = json.load(f)


class JsonFolder(object):
    """ Simple class for saving objects to a folder as json """
    def __init__(self, folder):
        self.folder = folder
        
    def save(self, filename, obj):
        with open(self.file(filename + ".json"), "w") as f:
            json.dump(obj, f)
            
    def load(self, filename):
        with open(self.file(filename + ".json"), "r") as f:
            return json.load(f)
        
    def file(self, filename):
        return path.join(self.folder, filename)
                
cache = JsonFolder("C:/Users/wb390262/Documents/Miscpy/json")
        
def get_country_info_fuzzy(s):
    iso3, fuzzy = Country.get_iso3_country_code_fuzzy(s)
    if iso3:
        return Country.get_country_info_from_iso3(iso3)

#countries = ["Cambodia", "China", "DPR Korea", "Indonesia", "Lao PDR", "Malaysia", "Mongolia", "Myanmar", "Papua New Guinea", "Philippines", "Thailand", "Timor‑Leste", " Viet Nam", "Cook Islands", "Fiji", "Kiribati", "Marshall Islands", "Micronesia, F. S.", "Nauru", "Niue", "Palau", "Samoa", "Solomon Islands", "Tokelau", "Tonga", "Tuvalu", "Vanuatu"]
east_asia_codes = ["KH", "CN", "KP", "ID", "LA", "MY", "MN", "MM", "PG", "PH", "TH", "TL", "VN", "CK", "FJ", "KI", "MH", "FM", "NR", "NU", "PW", "AS", "SB", "TK", "TO", "TV", "VU"]
east_asia = [Country.get_country_info_from_iso2(code) for code in east_asia_codes]
gpe_country_string="""Afghanistan;Albania;Bangladesh;Benin;Bhutan;Burkina Faso;Burundi;Cabo Verde;Cambodia;Cameroon;Central African Republic;Chad;Comoros;Congo, Democratic Republic of;Congo, Republic of;Cote d'Ivoire;Djibouti;Dominica;Eritrea;Ethiopia;The Gambia;Georgia;Ghana;Grenada;Guinea;Guinea-Bissau;Guyana;Haiti;Honduras;Kenya;Kiribati;Kyrgyz Republic;Lao PDR;Lesotho;Liberia;Madagascar;Malawi;Mali;Marshall Islands;Mauritania;FS Micronesia;Moldova;Mongolia;Mozambique;Myanmar;Nepal;Nicaragua;Niger;Nigeria;Pacific Islands;Pakistan;Papua New Guinea;Rwanda;Saint Lucia;Saint Vincent and the Grenadines;Sao Tome and Principe;Senegal;Sierra Leone;Somalia;South Sudan;Sudan;Tajikistan;Tanzania;Timor-Leste;Togo;Uganda;Uzbekistan;Vanuatu;Vietnam;Yemen;Zambia;Zimbabwe"""
# This list includes "Pacific Islands" and some of the country names
# instead, we want all of the eligible Pacific island country names, which
# will create some duplication (e.g. Marshall Is appears in both lists)

pacific_islands_string = """Federated States of Micronesia;Kiribati;Marshall Islands;
                   Nauru;Palau;Fiji;Papua New Guinea;Solomon Islands;Vanuatu;
                   Niue;Samoa;Tonga;Tuvalu"""
eligible_pacific_islands_string = """Federated States of Micronesia;Kiribati;Marshall Islands;Tonga;Tuvalu;Samoa;Solomon Islands;Vanuatu"""

# We ignore Pacific islands that are not eligible


# Note, GPE works at the sub-national level in some cases:
# Somalia - Puntland, Somaliland and Federal
# Tanzania - Mainland and Zanzibar
# Nigeria - states (which?)
# Pakistan - states (which?)
# In most international sources, sub-national data will not be available for
# these entities, so we ignore them for now.

# TODO: cache this country info in json!

def get_uis_dictionary(filename="uis-data-dictionary-education-statistics.xlsx",
                       sheet_name="Students and Teachers"):
    """ Import the UIS data dictionary from a spreadsheet
    Currently (2019-05-03) available at https://apiportal.uis.unesco.org/user-guide
    http://uis.unesco.org/sites/default/files/documents/uis-data-dictionary-education-statistics.xlsx
    """
    df = pd.read_excel(filename, sheet_name=sheet_name)
    
    # Replace the column headings with 2nd row where useful
    for i, col in enumerate(df.columns):
        if pd.isnull(df.iloc[0, i]):
            df.iloc[0, i] = col
    df.columns = df.iloc[0]
    
    # Remove empty lines and reindex
    df = df[pd.notnull(df["Indicator ID"])].reset_index(drop=True)
    
    # Fill down empty cells in some columns
    fill_cols = ['Dataflow / Dataset', 'Theme', 'Indicator Section', 'Table query']
    df[fill_cols] = df[fill_cols].fillna(method="ffill")
    
    return df

gpe_country_names = gpe_country_string.split(";") + eligible_pacific_islands_string.split(";")


def country_dict(names):
    """ For a list of country names, get full information about them and return
    a dict with ISO2 codes as keys and the full information dictionary as values """
    r = {}
    for n in names:
        iso3 = Country.get_iso3_country_code_fuzzy(n)[0]
        if iso3:
            country = Country.get_country_info_from_iso3(iso3)
            iso2 = country["#country+code+v_iso2"]
            country["iso3"] = iso3
            country["iso2"] = iso2
            
            # todo: make a separate list of preferred short names for 
            # visualization and tabulation
            country["viz"] = country['#country+name+preferred']
            r[iso2] = country
    return r

gpe_countries = country_dict(gpe_country_names)        
country_codes = gpe_countries.keys()
gpe_country_names2 = [country["#country+name+preferred"] 
                    for k, country in gpe_countries.items()]
                         
const_params = {
    "subscription-key": "8be270194d6444189bdde1a7b2666911",
    "format": "sdmx-json"
}

root_url = "http://api.uis.unesco.org/sdmx/"
defaults = {
    "data": {},
    "dataflow": {"references": "datastructure"}
}
    
templates = {
    "data": "data/UNESCO,EDU_NON_FINANCE,3.0/{filters}",
    "dataflow": "dataflow/UNESCO/EDU_NON_FINANCE/latest"
}


def construct_query(query_type, quiet=False, **kwargs):
    """ Create a URL based on templates, defaults and root_url above and
    fields specified as kwargs. kwargs that are present as string fields in 
    the URL template will be added first. 
    Any remaining kwargs will be added as URL query parameters.
     """
    
    # replace kwargs found in templates
    url_end = templates[query_type]
    url = urljoin(root_url, url_end.format_map(kwargs))
    fields = [fname for _, fname, _, _ in Formatter().parse(url_end) if fname]
    remaining = {k: v for k, v in kwargs.items() if not k in fields}
    params = {**const_params, **defaults[query_type], **remaining}
    if not quiet:
        print("- yr url is {url} with params {params}".format(url=url, params=params))
    return url, params

def get_json(query_type, quiet=False, **kwargs):
    """ Construct a URL based on the query type and kwargs
    and return the JSON found there """
    if not quiet:
        print("getting json", query_type, kwargs)
    url, params = construct_query(query_type, quiet=quiet, **kwargs)
    return requests.get(url, params=params).json()
    
# See here https://apiportal.uis.unesco.org/query-builder for example queries:
# e.g. https://api.uis.unesco.org/sdmx/data/UNESCO,EDU_NON_FINANCE,2.0/OFST+ROFST+ROFST_PHH.PT+PER.L1+L2._T....._Z..._T.........?format=sdmx-json&startPeriod=2012&endPeriod=2017&locale=en&subscription-key=8be270194d6444189bdde1a7b2666911
# (but this query is too large to return! Need to set countries)
# https://api.uis.unesco.org/sdmx/data/UNESCO,EDU_NON_FINANCE,2.0/OFST+ROFST+ROFST_PHH.PT+PER.L1+L2._T....._Z..._T.........AIMS_EAS_PAC?format=sdmx-json&startPeriod=2012&endPeriod=2017&locale=en&subscription-key=8be270194d6444189bdde1a7b2666911

dimension_ids_v2 = [
 'STAT_UNIT',
 'UNIT_MEASURE',
 'EDU_LEVEL',
 'EDU_CAT',
 'SEX',
 'AGE',
 'GRADE',
 'SECTOR_EDU',
 'EDU_ATTAIN',
 'WEALTH_QUINTILE',
 'LOCATION',
 'EDU_TYPE',
 'EDU_FIELD',
 'SUBJECT',
 'INFRASTR',
 'SE_BKGRD',
 'TEACH_EXPERIENCE',
 'CONTRACT_TYPE',
 'COUNTRY_ORIGIN',
 'REGION_DEST',
 'REF_AREA',
 'TIME_PERIOD'] 

dimension_ids = [
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
        ]

# note, obtained this by querying data_structure - see below.
# got the v3 structure from the query builder https://api.uis.unesco.org/sdmx/dataflow/UNESCO/EDU_NON_FINANCE/3/?format=sdmx-2.1&detail=full&references=all&locale=all&subscription-key=8be270194d6444189bdde1a7b2666911
# it is the same but with the addition of IMM_STATUS; also the return structure
# probably differs

def lod_to_csv(lod, filename, fieldnames=None):
    """ Write a list of dicts to a CSV file """
    if not fieldnames:
        fieldnames = lod[0].keys()
    with open(filename + ".csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames)
        writer.writeheader()
        for d in lod:
            writer.writerow(d)


def dimension_string_to_dict(s):
     """ Turn an indicator ID in the appropriate order into a dict of dimensions.
     Includes however many dimensions are given in the string. """
     return dict(zip(dimension_ids, s.split(".")))
    
    
def dict_to_dimension_string(d, ref_area=False, time_period=False):
    """ Turn a dictionary of dimensions into a string to use as an indicator ID
    in the appropriate order. Optionally include the ref area and time period """
    r = []    
    for dimension_id in dimension_ids:
        if not((dimension_id == "REF_AREA" and ref_area is False) or
               (dimension_id == "TIME_PERIOD" and time_period is False)):
            
            v = d.get(dimension_id, d.get(dimension_id.lower(), None))
            if v is None:
                r.append("")
            elif type(v) == list:
                r.append('+'.join(v))
            else:
                r.append(str(v))
    return ".".join(r)

def make_filters(**kwargs):
    r = []
    for d in dimension_ids:
        if d.lower() in kwargs:
            v = kwargs[d.lower()]
            if type(v) == list:
                r.append('+'.join(v))
            else:
                r.append(str(v))
        else:
            r.append("")  
    return ".".join(r)


def get_data(quiet=False, **kwargs):
    """ Make filters and pass remaining parameters to the json query
    TODO: TIME_PERIOD cannot be coded here 
    """
    kwargs = {k.lower(): v for k, v in kwargs.items()}
    filters = []
    for d in dimension_ids:
        d = d.lower()
        if d in kwargs:
            v = kwargs[d]
            if type(v) == list:
                filters.append('+'.join(v))
            else:
                filters.append(str(v))
            del(kwargs[d])
        else:
            filters.append("")  
    
    kwargs = {inflection.camelize(k, uppercase_first_letter=False): v
              for k, v in kwargs.items()}
    return get_json("data", quiet=quiet, filters='.'.join(filters), **kwargs)



def get_dimensions_alt(**kwargs):
    """ Submit a keys-only request to find out what range of other dimensions
    are available within the fields specified by the kwargs 
    Returns a dictionary e.g. {"REF_AREA": ["BD", "IN"...]} 
    
    NOTE: The new version of this function seems to return the same results
    and is quicker. May be worth checking against this old version
    occasionally.
    """
    message = get_data(detail="serieskeysonly", **kwargs)
    print("message length - v1", len(json.dumps(message)))
    obs = message["dataSets"][0]["series"]
    dimensions = message["structure"]["dimensions"]["series"]
    dimensions = [(d["id"], d["values"]) for d in dimensions]
    r = {id_: set() for (id_, _) in dimensions}
    for k, v in obs.items():
        for dimension_value, (id_, values) in zip(k.split(":"), dimensions):
            r[id_].add(values[int(dimension_value)]["id"])
    
    return {k: list(v) for k, v in r.items()}
    
    
def get_dimensions(**kwargs):
    """ Submit a keys-only request to find out what range of other dimensions
    are available within the fields specified by the kwargs 
    Returns a dictionary e.g. {"REF_AREA": ["BD", "IN"...]} 
    
    Note: returns an empty list for the time period. This information is not
    available in the query. Presumably need to obtain the actual data to get this?
    """
    message = get_data(detail="serieskeysonly", dimension_at_observation='AllDimensions', **kwargs)
    dimensions = message["structure"]["dimensions"]["observation"]
    return {d["id"]: [v["id"] for v in d["values"]] for d in dimensions}

def get_combos(**kwargs):
    """ Recursive generator to get all combinations of dimensions fitting the specified
    restrictions. 
    """
    #print("--get_combos", kwargs)
    leave_out = ["COUNTRY_ORIGIN", "REF_AREA", "TIME_PERIOD"]
    #include_dimids = [id for id in dimension_ids if id not in leave_out]
    message = get_data(detail="serieskeysonly", 
                       quiet=True,
                       dimension_at_observation='AllDimensions', 
                       **kwargs)
    dims = message["structure"]["dimensions"]["observation"]
    undetermined = {}
    determined = {}
    for dim in dims:
        dim_id = dim["id"]
        value_ids = [v["id"] for v in dim["values"]]
        if dim_id not in leave_out:
            if len(value_ids) == 0: # shouldn't happen
                raise ValueError("No values found for {}".format(dim_id))
            elif len(value_ids) == 1:
                determined[dim_id] = value_ids[0]
            else:
                undetermined[dim_id] = value_ids    
    if not undetermined:
        yield determined
    else:
        first_dim, first_value_ids = next(iter(undetermined.items()))
        for value_id in first_value_ids:
            determined[first_dim] = value_id
            if len(undetermined) == 1:
                yield determined
            else: # >1 undetermined value, so we need to do another query
                for combo in get_combos(**determined):
                    yield combo
                
        
            
        
            
def write_combos(filename, **kwargs):
    """ Wrapper for get_combos that writes to a text file and prints output"""
    with open(filename, "w") as f:
        for i, combo in enumerate(get_combos(**kwargs)):
            s = dict_to_dimension_string(combo)
            print("{}: {}".format(i, s))
            f.write("{}\n".format(s))
    
def get_written_combos(filename):
    """ Return list of dicts based on text file with one line per indicator """
    with open(filename, "r") as f:
        return [dimension_string_to_dict(line.strip()) for line in f]
                
def remove_dimension(code, i):
    """ remove ith dimension from a dimension string code """
    return ".".join(("*" if i == j else v)
                    for j, v in enumerate(code.split(".")))
        
def simplify_combos(combos, quiet=False):
    """ 
    
    
    
    Note: I tried doing this for one stat_unit at a time to try and
    reduce the comparisons for fnmatch, but this seems just to make
    it even slower
    
    """
    use_dimensions = [i for i, d in enumerate(dimension_ids)
        if not d == "STAT_UNIT" and any((d in combo) for combo in combos)]
    #use_dimensions.reverse()
    codes = [dict_to_dimension_string(c) for c in combos]
    original_codes = codes[:]
    # we step back through the dimensions, trying to remove them one
    # at a time, making sure the codes are still unique each time
    for i in use_dimensions:
        print("simplify_combos", i)
        new_codes = []
        for code in codes:
            new = remove_dimension(code, i)
            matches = fnmatch.filter(original_codes, new)
            if len(matches) == 1:
                new_codes.append(new)
            else:
                new_codes.append(code)
        codes = new_codes
    return [c.replace("*", "").strip(".") for c in codes]
                     
def abbreviate(simp_codes):
    """ Suggest a list of abbreviations based on simplified codes 
        NO LONGER USED - DELETE """
    remove = ["", "PT", "_T", "_Z", "W00"]
    r = []
    for code in simp_codes:
        new_code = "-".join([d for d in code.split(".") if not d in remove])
        r.append(new_code)
    return r

def abbreviate_combo(combo):
    """ Suggest a list of abbreviations based on removing defaults / totals """
    remove = ["_T", "_Z", "INST_T", "W00", "_X", "PT", "SCH_AGE_GROUP"]
    dont_remove = [("EDU_FIELD", "_X")]   # over-rides remove in specific cases
    mangle = ["_U"]  # alter unknown to a field-specific code
    replace = {"EDU_LEVEL": ("L", "")}
    r = {}
    for k, v in combo.items():
        if v in remove and not (k, v) in dont_remove:
            r[k] = ""
        elif v in mangle:
            r[k] = "{k}_{v}".format(k=k, v=v)
        elif k in replace:
            r[k] = v.replace(*replace[k])
        else:
            r[k] = v
    return r

def abbreviate_sdmx_key(key):
    """ Remove all excess dots """
    return "-".join(part.lower() for part in key.split(".") if part)


def match_combo_unique(combo, all_combos):
    """ Find a unique combo that matches an abbreviated combo.
    If more than one combo matches return None """
    m = match_combo_gen(combo, all_combos)
    r = next(m)
    try:
        problem = next(m)
    except StopIteration:
        return r    
    
def unabbreviate_sdmx_key(abbreviation, all_combos):
    """ Unabbreviate a key abbreviated using the above... 
    In fact as the abbreviation functions are fast the easiest way is to 
    create a list of the abbreviations and match against it!    
    """
    parts = abbreviation.split("-")
    
    # add the l back in the level
    
    # check if it identifies a unique combo
    
    # progressively restore removed values until it matches a unique combo
    
    return combo

def simplify_combos_old(combos, quiet=False):
    """ Simplify a list of combos by removing values that aren't needed 
    (first attempt: slightly brute force approach) 
    Note - we don't allow it to remove stat_unit - hard to identify
    what indicator it is without that.
    TODO: fix - not working correctly at present - 
    Problem is comparing e.g.:
    AIR.GPI.L1._T._T.TH_ENTRY_AGE.G1.INST_T._Z._Z._T.INIT._T._Z._Z._Z._Z._Z..W00._Z
    AIR.GPI.L1._T._T._T.GLAST.INST_T._Z._Z._T.INIT._T._Z._Z._Z._Z._Z..W00._Z
    -- both simplify to AIR.GPI. More than one other dimension varies between the 
    two, but when looking at the dimensions individually, we only find one unique
    'all but' value. We need to keep *either* TH_ENTRY_AGE vs. _T *or*
    G1 vs. GLAST. By default keep the first one?
    Start by counting all matches to the first dimension, then D1+D2, then D1+D2+D3
    - continue adding dimensions until there are no matches.
    *Then*, check if any of the middle dimensions can also be dropped??
    """
    # Make a dict of dimstrings ignoring one dimension at a time
    use_dimensions = []
    for d in dimension_ids:
        if not d == "STAT_UNIT" and any((d in combo) for combo in combos):
            use_dimensions.append(d)
            
    all_but = {}
    counts = {}
    for d in use_dimensions:
        if not quiet:
            print("listing all but", d)
        all_but[d] = [dict_to_dimension_string({k: v for k, v in combo.items() if k != d})
                for combo in combos]
        counts[d] = Counter(all_but[d])
    # Remove dimensions that are not necessary because there is only one 
    # 'all but' for each indicator
    for d in use_dimensions:
    # list of dim strings excluding the current dimension
        if not quiet:
            print("Simplifying dimension", d)
        for combo, code in zip(combos, all_but[d]):
            if counts[d][code] == 1:
                del combo[d]
    return all_but, counts
    

def simplify_sdmx(message, hide_na=True, hide_total=True):
    """ Convert an SDMX message in to a more human-readable list of dicts containing
    dimensions and values 
    TODO: set na or default values for all fields:
        'AGE': 'SCH_AGE_GROUP' (for NERA - but different elsewhere??)
        'SECTOR_EDU': 'INST_T'
        'COUNTRY_ORIGIN': 'W00'
        'REGION_DEST': 'W00'
    """
    obs = message["dataSets"][0]["observations"]
    dimensions = message["structure"]["dimensions"]["observation"]
    dimensions = [(d["id"], d["values"]) for d in dimensions]
    attributes = message["structure"]["attributes"]["observation"]
    attributes = [(d["id"], d["values"]) for d in attributes]
    r = []
    for k, v in obs.items():
        d = {"value": v[0]}
        for dimension_value, (id_, values) in zip(k.split(":"), dimensions):
            dim_id = values[int(dimension_value)]["id"]
            if not((hide_na and dim_id == "_Z") 
                    or (hide_total and dim_id == "_T")):
                d[id_] = dim_id
        for attribute_value, (id_, values) in zip(v[1:], attributes):
            d[id_] = values[attribute_value]["name"]
        r.append(d)
    return r
            
def convert_sdmx(message, value_list=False, shorthand=False):
    """ Convert SDMX into an indicator-country-values with exceptional 
    metadata format 
    TODO: check if there are cases when >1 dataset is returned! 
    TODO: allow indicator shorthands... based on a lookup from a CSV
    TODO: probably add the full indicator metadata at the end of the message
    """
    obs = message["dataSets"][0]["observations"]
    dimensions = message["structure"]["dimensions"]["observation"]
    dimensions = [(d["id"], d["values"]) for d in dimensions]
    attributes = message["structure"]["attributes"]["observation"]
    attributes = [(d["id"], d["values"]) for d in attributes]
    
    # Return an ind: {country: {year: value}} nested dictionary
    r = defaultdict(lambda: defaultdict(dict))
    for k, v in obs.items():
        # We convert a numberical key like 0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0
        # into a dictionary of dimensions and then back into a more readable
        # string like NERA.PT.M..., 
        dim_dict = {}
        for dimension_value, (id_, values) in zip(k.split(":"), dimensions):
            dim_dict[id_] = values[int(dimension_value)]["id"]
        indicator_id = dict_to_dimension_string(dim_dict)
        # ignoring attributes for now - add the exception checking later!
        ref_area = dim_dict["REF_AREA"]
        time_period = dim_dict["TIME_PERIOD"]
        r[indicator_id][ref_area][time_period] = v[0]
    return r

def drop_common_dimensions(combos):
    """ Drop any dimension that is always the same for a given
    stat_unit; works in place and returns a list of the dropped
    dimensions """
    use_dimensions = []
    for d in dimension_ids:
        if not d == "STAT_UNIT" and any((d in combo) for combo in combos):
            use_dimensions.append(d)
    stat_units = set(c["STAT_UNIT"] for c in combos)
    drop = []
    for stat_unit in stat_units:
        for d in use_dimensions:
            values = set(c[d] for c in combos if c["STAT_UNIT"] == stat_unit)
            if len(values) == 1:
                drop.append((stat_unit, d, next(iter(values))))
    for c in combos:
        for stat_unit, d, v in drop:
            if c["STAT_UNIT"] == stat_unit and c[d] == v:
                c[d] = ""    
    return drop
                


all_data = {}
inds = ["OFST", "ROFST", "ROFST_PHH"]
#country_codes = ["BD"]
"""
for country in country_codes:
    #filters = make_filters(stat_unit=inds, ref_area=country)
    print("Getting data for {}".format(country))
    all_data[country] = get_data(stat_unit=inds, ref_area=country)
    
    #dataflow = get_json("dataflow")
nera_bd=get_data(stat_unit="NERA",
         ref_area=["BD", "UG"],
         unit_measure="PT",
         start_period=2012,
         end_period=2016,
         dimension_at_observation='AllDimensions')
#TODO: consider getting data without AllDimensions - will require 
# different processing.
nera_bd1=get_data(stat_unit="NERA",
         edu_level="L1", unit_measure="PT",
         ref_area="BD",
         dimension_at_observation='AllDimensions')

obs = nera_bd["dataSets"][0]["observations"]
dimensions = nera_bd["structure"]["dimensions"]["observation"]
attributes = nera_bd["structure"]["attributes"]["observation"]
simp = simplify_sdmx(nera_bd)
c = convert_sdmx(nera_bd)
df = pd.DataFrame(simp)

dims = get_dimensions(stat_unit="NERA")
"""

#cache.save('all-dimensions', get_dimensions())  # slow!
#all_dims = cache.load('all-dimensions')
#write_combos(cache.file("nera-paths.txt"), stat_unit="NERA")
#write_combos(cache.file("all-paths.txt"))  # takes 15 minutes

combos = get_written_combos(cache.file("all-paths.txt"))
combos_copy = [c.copy() for c in combos]
dropped = drop_common_dimensions(combos_copy)
abb_combos = [abbreviate_combo(combo) for combo in combos_copy]

full_sdmx_keys = [dict_to_dimension_string(combo) for combo in combos]
abb_sdmx_keys = [dict_to_dimension_string(c) for c in abb_combos]
abb_sdmx_keys2 = [abbreviate_sdmx_key(k) for k in abb_sdmx_keys]

for c, full, abb, abb2 in zip(combos, full_sdmx_keys, abb_sdmx_keys, abb_sdmx_keys2):
    c["full_sdmx_key"] = full
    c["abb_sdmx_key"] = abb
    c["abb_sdmx_key2"] = abb2

# identify duplicates by abb_sdmx_keys2 and write to CSV
counts = Counter(abb_sdmx_keys2)
dups = [c for c in combos if counts[c["abb_sdmx_key2"]] > 1]
fieldnames = dimension_ids + ["full_sdmx_key", "abb_sdmx_key", "abb_sdmx_key2"]
lod_to_csv(dups, "duplicate_abb_codes2", fieldnames)
lod_to_csv(combos, "all combos", fieldnames)

""" Removed all duplicates by abb_sdmx_key.
    
# identify duplicates by abb_sdmx_keys and write to CSV
counts = Counter(abb_sdmx_keys)
dups = [c for c in combos if counts[c["abb_sdmx_key"]] > 1]
lod_to_csv(dups, "duplicate_abb_codes2", 
           fieldnames=dimension_ids + ["full_sdmx_key", "abb_sdmx_key"])
"""

""" Issues causing duplicates:
    1. FEP - EDU_FIELD = _T or _X (Total or Unspecified)
    Is there really data for both of these?
    _X is not given in the data dictionary
    Consider not removing _X s throughout, or only in EDU_FIELD
    
    
    2. RPTR - GRADE = _T or _U (Total or Unknown)
    Unknown represents numbers of pupils known to have repeated but
    where it is not known what grade they are in! So should not be
    excluded. Consider not removing _U throughout, or only in GRADE
    
    3. STU - GRADe = _T or _U
    4. TEACH - EDU-ATTAIN = _T or _U
    5. TEACH - AGE = _T or _U
    
        
#orig_combos = [dict_to_dimension_string(combo) for combo in combos]
#new_codes3 = simplify_combos(combos)
#abbreviations = abbreviate(new_codes3)
#counts = Counter(abbreviations)
#dups = [(code, a) for code, a in zip(new_codes3, abbreviations) if counts[a] > 1]


# an actual project... obtain data on OOSC for all GPE DPCs


#data_structure = dataflow["DataStructure"][0]
#dimensions = data_structure["dimensionList"]["dimensions"]
# dimension_ids = [d["id"] for d in dimensions]
"""

"""
Update 2019-05-03
UIS has published the data dictionary in Excel!
https://apiportal.uis.unesco.org/user-guide
http://uis.unesco.org/sites/default/files/documents/uis-data-dictionary-education-statistics.xlsx

It should do this in json or yaml...
Note it also has the SDG4 indicators. (Are these repeated in Students and Teachers?)
some of the indicator IDs are just numbers.
Others are similar to simplified versions of the flowrefs, but slightly different...
e.g. OFST.1.cp  

There are a lot more codes in the UIS dictionary than combos I found in the 
actual API.

Notes on terminology
The set of dimension-value pairs as a string is called a 'key of the series'
 (https://apiportal.uis.unesco.org/docs/services/58079a0c8a75eb0d3c057522/operations/59bae6a7d8ae921a18d4bf68?)
so could refer to these as 'series_key'. (Though slightly confusing because multiple
series may be returned.)
However UIS simply refers to them as indicators...
call it the 'sdmx key'


Data from URI using v3:
Going from specific to general - this is the query for Bangladesh primary net
attendance rate in 2017-2018 - all
parameters set to not applicable or total where they don't apply.

https://api.uis.unesco.org/sdmx/data/UNESCO,EDU_NON_FINANCE,3.0/NERA.PT.L1._T._T.SCH_AGE_GROUP._T.INST_T._Z._Z._T._T._T._Z._Z._Z._Z._Z....BD?startPeriod=2017&endPeriod=2018&format=sdmx-json&locale=en&subscription-key=8be270194d6444189bdde1a7b2666911

Structure is like this - 
["dataSets"]["series"][""0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0"]["observations"]["0"]
and
["dataSets"]["series"][""0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0"]["observations"]["1"]
each contain the relevant list of 5 values

One problem with this SDMX is that it does not seem to distinguish 
between data points that should be available but are not, and data points
that would not make sense. Don't know if there are any examples of data points
that would make sense but are not available for any country.

TODO: utility functions for extracting useful simplified datasets including:
    - latest data (with year) for all countries


/ TODO: a recursive function that will step through each dimension and build a complete list 
of the possible combinations. (I'm assuming that these combinations will have actual data)
Do not include ref_area or COUNTRY_ORIGIN, which will have a large number of options; or time_period.

/ TODO: for all the combos find out the minimum reference that will get
a message with uniquely that combo. (Also remove excess dots)
Note, now not sure how useful this is. These codes may change if there is any
change to indicator availability.
Consider focusing on simplifying names instead by setting default values under
each dimension. But it is not certain this will work, as the defaults for
each dimension depend on which other values are set in other dimensions. e.g.
for some indicator PT is the default unit, for others perhaps NB, but some
may have both. Progressively remove defaults, checking uniqueness each time?
This can still potentially break if there is a change. Need to hard-code
the abbreviated indicator names in a table rather than expecting an 
algorithm to pull them out reliably. - *unless* the SDMX specifies the full
set of possible indicators somewhere, as opposed to those that actually have
data points. But it might be hard to specify in advance the full set possible.
    
TODO: find a way to describe each combo using the combination of full dimension
names and values.

TODO: try a way of generating abbreviated names automatically:
    - take the simplified name
    - remove all multiple dots, _T, _Z and W00
    - check uniqueness
Result: the following are duplicated (2 or 3 cases of each)
    ['GER-L2_3-M',
 'GER-L2_3-F',
 'STU-GPI',
 'TEACH-L2_3-_U',
 'TEACH-C4-_U',
 'TEACH-C5-_U',
 'TEACH-L02-_U',
 'TEACH-L3-_U',
 'TEACH-L2-_U',
 'TEACH-L1-_U']
    
Note the simplify algorithm has not always picked out the dimensions of interest.
e.g. GER-L2_3-M - one of these is for underage while the other is for sch_age_group.
But the algorithm instead picks up on the wealth quintile, coded as _Z for underage
(i.e. there will never be wealth disaggregation) and _T for sch_age_group (there 
could in theory by disaggregation). (Not clear if this coding is 'correct' - 
both are from admin data, and are unlikely ever to be disaggregated because
you would need matching wealth data in the school census and person census,
but we have it in neither.) These values are then removed as they are both
defaults. Possibly, we can avoid this by getting the simplify algorithm
to ignore _T, _Z when simplifying? But it is possible there are cases that rely
on the difference between _T and _Z.

This abbreviation is important because we also want to be able to generate
abbreviated descriptions of the indicators
e.g. 'Gross enrolment ratio, Secondary school, Male' that leaves out anything
that is _Z or _T. 

Note, SDMX seems flawed if you want to get more than 1 indicator at a time
and they are not related to each other. e.g. you can easily get all the
GERs with stat_unit=GER or the GER and other stats for girls using
stat_unit=GER+NER+NARA&sex=F. But it would be harder to get miscellaneous
variables some gender disaggregated and some not... you would end up getting
things you don't need.

When I add PT to the list being removed then there are 57 dups
Try to fix the dups by replacing the 2nd dup with its non-removed value



TODO: get data as dataframe     
    1. format="long" -- basic long: ref_area | time_period | indicator | value
    2. "time period" -- wide: ref_area | indicator | 1995 | 1996 etc. for 1+ indicators
    3. "stat unit" -- wide: country | year | NERA | NARA etc. for 1+ years
    4. "combined" -- Combined wide: country | NERA_1995 | NERA_1996 etc. | NARA_1995 | NARA_1996...
(This should cover most cases. Rarely want countries as column heading)
Note hdf offers a way of progressively appending bits of data to a single
dataframe on disk http://pandas-docs.github.io/pandas-docs-travis/user_guide/io.html#table-format

TODO: conversion functions between my ind-country-year format and the 
different types of dataframe

TODO: a function for combining two or several simplified ICY datasets
Then can stitch together datasets that are too large to grab in one go
 
TODO: write a Stata/R module to grab this data too... As dta? or csv? 
Do not want to parse the json in Stata, so could do this by making a py
command line utility which Stata and R can access.
NB there are WB data tools for R and Py
https://cran.r-project.org/web/packages/wbstats/index.html
https://cran.r-project.org/web/packages/WDI/index.html
https://github.com/OliverSherouse/wbdata
https://datahelpdesk.worldbank.org/knowledgebase/articles/1886701-sdmx-api-queries
WB has both a simple API (and several wrappers for that) and an SDMX one
(currently providing WDI only). A good aim would be to translate the UIS
SDMX API into the WB form. WB query format is not ideal but would make it 
easier for people to make the transition
https://datahelpdesk.worldbank.org/knowledgebase/articles/898581-api-basic-call-structures

see also https://cran.r-project.org/web/packages/RJSDMX/index.html

TODO: for publication of the py tools, include a cached list of all indicators,
and a function to update the list. It probably won't change much until
there is a new version of the API.

TODO: consider storing and manipulating indicator information in a pandas
dataframe instead - will want to save it to a CSV afterwards.

TODO: a 'dimension browser' where you could search for e.g. "_T" and it will 
list all the meanings of that value: "_T" can mean Sex: Total, Grades: Total, etc.


Framework for simplifying...
Each value will correspond to 3 dimensions:
    Country
    Year
    'Indicator' (everything else)
    
Everything else will be a dict so that it is easy to interrogate alternate
versions of the same indicator

Develop shorthands for common indicators which can be modified by adding
dimensions
e.g. primary-aner => NERA.PT.L1.SCH_AGE_GROUP.INST_T...
 primary-aner+male => the male version
 primary-aner+gpi => the GPI version
 
 Each addition causes a substitution in one or more dimensions.
 
 Make these in a CSV sheet. (Is it possible to get a complete listing of those for
 which data is available??)
 
 However I want to ensure that the shorthands all refer to an actual dimension.
 So aner by itself would not exist (because we don't have a combined ANER).
 But oosc by itself might exist.
 
 I also want to be able to pass incomplete queries, e.g. "NERA-L1". It will set non-determined values
 to sensible defaults to the extent possible. But note that some dimensions like
 age will differ according to the indicator. Would like to be able to query them
 without specifying. If all return values have the same dimensions then they can be returned
 as a single 'indicator'; if not then as multiple indicators.
 
The return message will have one or more indicators. Within each indicator provide a dict of
{country_id: [list of values], country_id2: [list of values], start_period: 2012, end_period: 2016,
     unit_mult: 'Units', obs_status: 'Normal', freq: 'Annual', decimals: 'Five', 
     exceptions: {dict of dicts of dicts of any cases where the attributes are not the same}}

An example of an exceptions dict could be {BD: {1990: {obs_status: {'Not normal'}}}} to show
that the observation status was not normal in Bangladesh in 1990.

Allow an alternative message format where the list of values is replaced
with a {year: value} dict - this will be more useful for sparser data.
(In fact, we will make this the default initially, as it is easier to
write such a dict first then convert it into a list afterwards.)

Note that we will need to encode the start_period and end_period found in the 
data, especially if returning the data in list format. These can be excluded
from the query, and even if included in the query, the start and end found
in the data may not match.

use the complete string like NERA.PT.L1...................BD. (but remove the country)
as the indicator ID for now. Later will want to change this to a more
human readable shorthand.

Also want... 
Metadata explorer
- a lookup function to find the different indicators e.g.
search for ANER and it will list all the options 
*** Note the new data browser does this, more or less!

- provide the number of countries with some non-missing data
- then allow to probe: number of years by country, number of countries with
1+, 2+, 10+ data points, number of countries by year

Encoding as strings for now. Consider encoding as float later?

A query without observations (detail=serieskeysonly) looks like the best way of finding what dimension
variation there is for a particular indicator, e.g.
https://api.uis.unesco.org/sdmx/data/UNESCO,EDU_NON_FINANCE,3.0/NERA.....................?startPeriod=2017&endPeriod=2018&format=sdmx-json&detail=serieskeysonly&locale=en&subscription-key=8be270194d6444189bdde1a7b2666911

not sure if this guarantees there is an actual observation in each case.

note there is also the parameter dimensionAtObservation: "AllDimensions"
which must affect what is returned... the structure is actually closer to what
I want when this is not specified!


Note, will want to add e.g. unesco-education to the indicator name in order 
to be able to use this for multiple data providers. Allow shorthands for the
providers as well as the indicators. Like a URL for every indicator!



"""