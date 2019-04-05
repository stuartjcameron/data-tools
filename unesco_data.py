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

#working_dir = "C:/Users/scameron/Dropbox (Personal)/Py"
#with open(path.join(working_dir, "country_2letter_codes.json"), "r") as f:
#    all_codes = json.load(f)

def country_lookup(search, first_word=False, any_word=False):
    table = {d["Name"].lower(): d["Code"] for d in country_codes}
    def search_word(w):
        w = w.lower()
        for name, code in table.items():
            if w in name:
                return code
    
    r = search_word(search)
    if r is not None:
        return r
    if any_word:
        for w in search.split():
            r = search_word(w)
            if r is not None:
                return r
    elif first_word:
        r = search_word(search.split()[0])
        if r is not None:
            return r
                
        
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

gpe_country_names = gpe_country_string.split(";") + eligible_pacific_islands_string.split(";")

def prop_list(list_or_dict, key):
    L = list_or_dict
    if type(L) is dict:
        L = list_or_dict.values()
    return [item[key] for item in L]

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
    "data": {
        
                   "dimensionAtObservation": "AllDimensions"},
        "dataflow": {"references": "datastructure"}
        }
    
templates = {
        "data": "data/UNESCO,EDU_NON_FINANCE,2.0/{filters}",
        "dataflow": "dataflow/UNESCO/EDU_NON_FINANCE/latest"
        }


def construct_query(query_type, **kwargs):
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
    print("- yr url is {url} with params {params}".format(url=url, params=params))
    return url, params

def get_json(query_type, **kwargs):
    """ Construct a URL based on the query type and kwargs
    and return the JSON found there """
    print("getting json", query_type, kwargs)
    url, params = construct_query(query_type, **kwargs)
    return requests.get(url, params=params).json()
    
# See here https://apiportal.uis.unesco.org/query-builder for example queries:
# e.g. https://api.uis.unesco.org/sdmx/data/UNESCO,EDU_NON_FINANCE,2.0/OFST+ROFST+ROFST_PHH.PT+PER.L1+L2._T....._Z..._T.........?format=sdmx-json&startPeriod=2012&endPeriod=2017&locale=en&subscription-key=8be270194d6444189bdde1a7b2666911
# (but this query is too large to return! Need to set countries)
# https://api.uis.unesco.org/sdmx/data/UNESCO,EDU_NON_FINANCE,2.0/OFST+ROFST+ROFST_PHH.PT+PER.L1+L2._T....._Z..._T.........AIMS_EAS_PAC?format=sdmx-json&startPeriod=2012&endPeriod=2017&locale=en&subscription-key=8be270194d6444189bdde1a7b2666911
dimension_ids = [
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

# note, obtained this by querying data_structure - see below.

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


def get_data(**kwargs):
    """ Make filters and pass remaining parameters to the json query
    TODO: - convert queries from _ to camel 
    """
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
    return get_json("data", filters='.'.join(filters), **kwargs)

# get all data... data_q = get_json("data", filters="all")
#filters = make_filters(stat_unit=["OFST", "ROFST", "ROFST_PHH"],
                       #ref_area=country_codes)
# this query is too large - get the data one country at a time instead
# data_q = get_json("data", filters=filters)
all_data = {}
inds = ["OFST", "ROFST", "ROFST_PHH"]
country_codes = ["BD"]
for country in country_codes:
    #filters = make_filters(stat_unit=inds, ref_area=country)
    print("Getting data for {}".format(country))
    all_data[country] = get_data(stat_unit=inds, ref_area=country)
    
    #dataflow = get_json("dataflow")
nera_bd=get_data(stat_unit="NERA",
         ref_area="BD",
         start_period=2012,
         end_period=2012)
nera_bd1=get_data(stat_unit="NERA",
         edu_level="L1",
         ref_area="BD",
         start_period=2012,
         end_period=2012)

#data_structure = dataflow["DataStructure"][0]
#dimensions = data_structure["dimensionList"]["dimensions"]
# dimension_ids = [d["id"] for d in dimensions]

"""
Data from function using v2:
In the data downloaded above, all_data["BD"]["dataSets"][0]["observations"] yields the
actual observations as a dict. The keys seem to refer to different dimension
combinations while the value is a list, the first value of which appears to be
the actual value of the indicator. Not sure what the other 4 values are.
It is providing every possible value of the 4 indicators...
all_data["BD"]["structure"]["dimensions"]["observation"][21] provides the time period
(You would have to search for a dict with id "TIME_PERIOD"! and the period
is provided as a list of dicts...

Data from URI using v3:
Going from specific to general - this is the query for Bangladesh primary net
attendance rate in 2017-2018 - all
parameters set to not applicable or total where they don't apply.
T
https://api.uis.unesco.org/sdmx/data/UNESCO,EDU_NON_FINANCE,3.0/NERA.PT.L1._T._T.SCH_AGE_GROUP._T.INST_T._Z._Z._T._T._T._Z._Z._Z._Z._Z....BD?startPeriod=2017&endPeriod=2018&format=sdmx-json&locale=en&subscription-key=8be270194d6444189bdde1a7b2666911

Structure is like this - 
["dataSets"]["series"][""0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0"]["observations"]["0"]
and
["dataSets"]["series"][""0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0"]["observations"]["1"]
each contain the relevant list of 5 values


Presumably v3 has different dimensions. I cannot make the query still work 
just by changing the version number.






"""