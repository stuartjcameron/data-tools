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
import pandas as pd
from file_utilities import Cache
import uis_api_wrapper as api

#working_dir = "C:/Users/scameron/Dropbox (Personal)/Py"
#with open(path.join(working_dir, "country_2letter_codes.json"), "r") as f:
#    all_codes = json.load(f)

cache = Cache("C:/Users/wb390262/Documents/Miscpy/json")

      
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

inds = ["OFST", "ROFST", "ROFST_PHH"]
#country_codes = ["BD"]
#all_data = {}

#for country in country_codes:
#    print("Getting data for {}".format(country))
#    all_data[country] = api.get_data(stat_unit=inds, ref_area=country)

nera_bd = api.get_data(stat_unit="NERA",
         ref_area=["BD", "UG"],
         unit_measure="PT",
         start_period=2012,
         end_period=2016,
         dimension_at_observation='AllDimensions')

#TODO: consider getting data without AllDimensions - will require 
# different processing.
nera_bd1 = api.get_data(stat_unit="NERA",
         edu_level="L1", unit_measure="PT",
         ref_area="BD",
         dimension_at_observation='AllDimensions')

obs = nera_bd["dataSets"][0]["observations"]
dimensions = nera_bd["structure"]["dimensions"]["observation"]
attributes = nera_bd["structure"]["attributes"]["observation"]



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

TODO: allow multiple queries. Note that there are 2 possible ways to pass 
the queries:
    1. combine them into a single query using +, which may get too much data in some
    cases, then filter out the unwanted data.
    2. (much easier and possibly better, but slower) query one at a time and combine
    the data sets.
    

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