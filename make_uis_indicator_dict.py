# -*- coding: utf-8 -*-
"""
Script that takes the UIS data dictionary (Students and Teachers), and adds
- a new type of shortened indicator key
- full SDMX key to get each indicator in the API
- some indicators found in the API but missing in the dictionary

It then saves this dictionary for use in the uis_indicators module

@author: scameron
"""

import pandas as pd
from collections import defaultdict, Counter
from file_utilities import Cache
import uis_api_wrapper as api
from uis_spec import UISSpec as Spec
from itertools import product
import logging as lg
from numpy import NaN
lg.basicConfig(level=lg.DEBUG)
#lg.getLogger().setLevel(lg.INFO)
cache = Cache("C:/Users/wb390262/Documents/Miscpy/json")


def get_uis_dictionary(filename="uis-data-dictionary-education-statistics.xlsx",
                       sheet_name="Students and Teachers"):
    """ Import the UIS data dictionary from a spreadsheet
    Currently (2019-05-03) available at https://apiportal.uis.unesco.org/user-guide
    http://uis.unesco.org/sites/default/files/documents/uis-data-dictionary-education-statistics.xlsx
    """
    df = pd.read_excel(filename, 
                       sheet_name=sheet_name, 
                       keep_default_na=False) 
    # by default pandas changes "NA" to N/A so we over-ride this then
    # change empty cells to N/A    
    df = df.replace("", NaN)
    
     # Fill down empty cells in some columns
    fill_cols = ['Dataflow / Dataset', 'Theme', 'Indicator Section', 'Table query']
    df[fill_cols] = df[fill_cols].fillna(method="ffill")
        
    # Replace the column headings with 2nd row where useful
    for i, col in enumerate(df.columns):
        if pd.isnull(df.iloc[0, i]):
            df.iloc[0, i] = col
    df.columns = df.iloc[0]
    
    # Remove empty lines and reindex
    df = df[1:][pd.notnull(df["Indicator ID"])].reset_index(drop=True)
    
    return df


def add_sdmx_keys(df):
    """ Add the sdmx keys to a dataframe. """
    df["sdmx_key"] = df[Spec.dims].apply(lambda x: ".".join(x), axis=1)

def match_inds_in_api(spec):
    """ Recursive generator to find all indicators that are in 
    the API and fit the given indicator specification
    """
    #print("--get_combos", kwargs)
    #leave_out = ["COUNTRY_ORIGIN", "REF_AREA", "TIME_PERIOD"]
    #leave_out = ["REF_AREA", "TIME_PERIOD"]
    #include_dimids = [id for id in dimension_ids if id not in leave_out]
    message = api.get_data(detail="serieskeysonly", 
                       quiet=True,
                       dimension_at_observation='AllDimensions', 
                       **kwargs)
    dims = message["structure"]["dimensions"]["observation"]
    undetermined = {}
    determined = {}
    for dim in dims:
        dim_id = dim["id"]
        value_ids = [v["id"] for v in dim["values"]]
        if dim_id not in Spec.ignore:
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
                for ind in match_inds_in_api(Spec(**determined)):
                    yield ind
                
           
def write_combos(filename, **kwargs):
    """ Wrapper for get_combos that writes to a text file and prints output"""
    with open(filename, "w") as f:
        for i, combo in enumerate(get_combos(**kwargs)):
            s = Spec(**combo).to_key()
            print("{}: {}".format(i, s))
            f.write("{}\n".format(s))
    
def get_written_combos(filename):
    """ Return list of dicts based on text file with one line per indicator """
    with open(filename, "r") as f:
        return [sdmx_key_to_dict(line.strip()) for line in f]
                
def remove_dimension(code, i):
    """ remove ith dimension from a dimension string code """
    return ".".join(("*" if i == j else v)
                    for j, v in enumerate(code.split(".")))
def abbreviate_combo(combo):
    """ Suggest a list of abbreviations based on removing defaults / totals """
    remove = ["_T", "_Z", "INST_T", "W00", "_X", "PT", "SCH_AGE_GROUP"]
    dont_remove = [("EDU_FIELD", "_X"), ("COUNTRY_ORIGIN", "PT")]   # over-rides remove in specific cases
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


def match_short_key(key, indicators):
    """ Find a combo in all_combos that matches the given abbreviated SDMX key """
    r = [ind for ind in indicators if ind["short_key"] == key]
    if len(r) > 1:
        raise KeyError("Short key matches more than one indicator")
    elif len(r) == 0:
        raise KeyError("Short key does not match any indicator")
    return r[0]


def unique(it):
    """ Return only a unique value and raise an error if there is >1 """
    v = None
    for i, v in enumerate(it):
        if i > 0:
            raise ValueError("More than one value was found in iterable")
    return v

def short_key_to_combo(key, possible, dropped_dims=None, quiet=True):
    """ Given all the possible values for each dimension,
    return possible combos matching a short key.
    Also uses dimension_ids """
    # The following have to be the same in in abbreviate_combo
    # TODO: move both into a class or similar 
    # cache and store these together with dimension_ids and all_dims
    remove = ["_T", "_Z", "INST_T", "W00", "_X", "PT", "SCH_AGE_GROUP"]
    dont_remove = [("EDU_FIELD", "_X"), ("COUNTRY_ORIGIN", "PT")]   # over-rides remove in specific cases
    spec_values = [part.upper() for part in key.split("-")]
    
    # First check the stat_unit which should be the first part
    stat_unit = spec_values.pop(0)
    if stat_unit not in possible["STAT_UNIT"]:
        return []
    
    dont_use = ["STAT_UNIT", "REF_AREA", "TIME_PERIOD"]
    dims = [d for d in dimension_ids if d not in dont_use]
    dropped_dict = {}
    if dropped_dims:
        for s, d, v in dropped_dims:
            if stat_unit == s:
                dropped_dict[d] = v
    
    def ordered(s):
        """ Check whether a list of dimensions are in the right order
        or not """
        return all(dims.index(a) < dims.index(b) 
                   for a, b in zip(s, s[1:]))
        
    def unspecified_value(d):
        """ Return possible unspecified values for a given dimension id """
        return list(set(v for v in remove if (d, v) not in dont_remove)
                    & set(possible[d]))

    possible_dims_for_each_value = []
    fixed = []
    for value in spec_values:
        possible_dims = [d for d in dims 
                         if value_matches_dim(value, d, possible[d]) 
                         and d not in fixed
                         and d not in dropped_dict]
        # Note: consider whether to allow the user 
        # to specify dropped values e.g. _Z
        # Then we would include the dropped dimension in possible_dims
        # as long as value_matches_dim == the dropped value
        possible_dims_for_each_value.append(possible_dims)
        if len(possible_dims) == 1:
            fixed.append(possible_dims[0])
    if not quiet:
        print("fixed", fixed)
        print("pdfev", possible_dims_for_each_value)
    r = []
    for dim_combination in product(*possible_dims_for_each_value):
        if ordered(dim_combination):
            dc = dict(zip(dim_combination, spec_values))
            dc["STAT_UNIT"] = stat_unit
            for d in dims:
                if d in dc:
                    dc[d] = value_matches_dim(dc[d], d, possible[d])    
                else:
                    dc[d] = unspecified_value(d)
            r.append(dc)     
    return r

            
        
def value_matches_dim(value, dim_id, possible):
    """ Check whether a given value can represent the given dimension
    given an array of possible values for the dimension. Return the 
    original value if so """
    # The following have to be the same in in abbreviate_combo
    # TODO: move both into a class or similar 
    # cache and store these together with dimension_ids and all_dims
    mangle = ["_U"]  # alter unknown to a field-specific code
    replace = {"EDU_LEVEL": ("L", "")}
    if value in possible:
        return value
    for mangled_value in set(mangle) & set(possible):
        if value == "{k}_{v}".format(k=dim_id, v=mangled_value):
            return mangled_value
    if dim_id in replace:
        for v in possible:
            if value == v.replace(*replace[dim_id]):
                return v
        
   

def shorten_sdmx_key(key):
    """ Remove all excess dots """
    return "-".join(part.lower() for part in key.split(".") if part)
 

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
        indicator_id = dict_to_sdmx_key(dim_dict)
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


def combine_combos(*combos):
    """ Combine combos into a single combo with multiple values.
    Note this can 'over-query', i.e. return too many results if there
    is too much difference between the combos. """
    r = defaultdict(set)
    for combo in combos:
        for key, value in combo.items():
            if type(value) is list:
                r[key] |= set(value)
            else:
                r[key].add(value)
    return {k: list(v) for k, v in r.items()}
            
        

#cache.save('all-dimensions', api.get_dimensions())  # slow!
all_dims = cache.load('all-dimensions')
#write_combos(cache.file("all-paths2.txt")) #takes 5 minutes

combos = get_written_combos(cache.file("all-paths2.txt"))
combos_copy = [c.copy() for c in combos]
dropped = drop_common_dimensions(combos_copy)
# note, dropping common dimensions makes the short name depend
# on all of the combinations that are available, so harder to 'undo'

abb_combos = [abbreviate_combo(combo) for combo in combos_copy]

full_sdmx_keys = [dict_to_sdmx_key(combo) for combo in combos]
abb_sdmx_keys = [dict_to_sdmx_key(c) for c in abb_combos]
short_keys = [shorten_sdmx_key(k) for k in abb_sdmx_keys]

for c, full, abb, abb2 in zip(combos, full_sdmx_keys, abb_sdmx_keys, short_keys):
    c["full_sdmx_key"] = full
    c["abb_sdmx_key"] = abb
    c["short_key"] = abb2



# identify duplicates by abb_sdmx_keys2 and write to CSV
counts = Counter(short_keys)
dups = [c for c in combos if counts[c["short_key"]] > 1]
fieldnames = dimension_ids + ["full_sdmx_key", "abb_sdmx_key", "short_key"]
cache.save("duplicate short codes", dups, fieldnames)
cache.save("all combos", combos, fieldnames)


# take an example and un-shorten it
s = unique(short_key_to_combo("cr-glpia-1-q3", all_dims, dropped))
data_message = api.get_data(**s)

d = {}
for k in short_keys:
    d[k] = short_key_to_combo(k, all_dims, dropped)

dups = {k: combos for k, combos in d.items() if len(combos) > 1}
# there are some duplicates for stat_unit TEACH
# where the age has been set to Y11t15 instead of teach_experience
# note that no such values exist in the data
# 'combining' the combos doesn't work because it will return
# TEACH_EXPERIENCE=_T as well as Y11T15
# instead we want to query both and combine the data
# the first with age=Y11t15 will not return anything.
uis_dictionary = get_uis_dictionary()
add_sdmx_keys(uis_dictionary)
key_lookup = dict(zip(full_sdmx_keys, short_keys)).get
uis_dictionary["short_key"] = uis_dictionary["sdmx_key"].apply(key_lookup.get)

# There are some duplicates...
uis_dictionary.drop_duplicates(subset="Indicator ID", inplace=True)
uis_dictionary.set_index("Indicator ID", drop=False, inplace=True)
#json = uis_dictionary.to_json(orient="index")
uis_dictionary.to_json(cache.file("uis_dictionary", fmt="json"), orient="index")
uis_dictionary.to_csv(cache.file("uis_dictionary", fmt="csv"))
keys_in_dict = list(uis_dictionary["sdmx_key"])
keys_not_in_dict = [k for k in full_sdmx_keys if k not in keys_in_dict]
not_in_api = uis_dictionary[~uis_dictionary["sdmx_key"].isin(full_sdmx_keys)]
# There are 45 cases where full key was not found in API but is there in database
# TODO: check what these are and check for errors in the non-found keys
# (If they are not in the API and there are no obvious errors in the spreadsheet,
# then keep these in the final dictionary)
# the stat_unit SLE_X25t99 does not exist in the API; should this have 
# a different age category instead??


# There are 6 cases where full keys are found in API but not in UIS dict
# These are teacher attrition for general (edu_cat=C4) and vocational (edu_cat=C5)
# programs separately. 
# /TODO: Check if there is actually any data for these. Add them to the final dictionary anyway
# There is actual data for these indicators. They should be in the final dictionary.



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
    
        
#orig_combos = [dict_to_sdmx_key(combo) for combo in combos]
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


TODO: for publication of the py tools, include a cached list of all indicators,
and a function to update the list. It probably won't change much until
there is a new version of the API.

TODO: consider storing and manipulating indicator information in a pandas
dataframe instead - will want to save it to a CSV afterwards.



Framework for simplifying...
Each value will correspond to 3 dimensions:
    Country
    Year
    'Indicator' (everything else)
    
Everything else will be a dict so that it is easy to interrogate alternate
versions of the same indicator
 
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

Also want... 
Metadata explorer
- a lookup function to find the different indicators e.g.
search for ANER and it will list all the options 
TODO: a 'dimension browser' where you could search for e.g. "_T" and it will 
list all the meanings of that value: "_T" can mean Sex: Total, Grades: Total, etc.
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