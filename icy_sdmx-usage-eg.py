# -*- coding: utf-8 -*-
"""
Created on Mon Jun  3 09:23:07 2019

Example: Get adjusted net enrolment rate for Bangladesh and Uganda,
2012-16.
@author: WB390262
"""
from file_utilities import Cache
from icy_sdmx import Api, sdmx_to_icy

cache = Cache("C:/Users/wb390262/Documents/Miscpy/json")

api = Api(base="https://api.uis.unesco.org/sdmx/data/UNESCO,EDU_NON_FINANCE,3.0", 
          subscription_key="8be270194d6444189bdde1a7b2666911",
          dimensions=[
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
            ])
api.verification = False  # not secure but need this at the moment 


nera_bd = api.query(stat_unit="NERA",
         ref_area=["BD", "UG"],
         unit_measure="PT",
         start_period=2012,
         end_period=2016,
         dimension_at_observation='AllDimensions')

nera_bd_icy = sdmx_to_icy(nera_bd, api.filter)

nera_bd_icy2 = api.icy_query(stat_unit="NERA",
         ref_area=["BD", "UG"],
         unit_measure="PT",
         start_period=2012,
         end_period=2016)

cache.to_json("nera_bd", nera_bd)
cache.to_json("nera_bd_icy", nera_bd_icy)


"""
# want a shorthand like
# nera_bd = api.get_icy("nera-pt", ["BD", "UG"], 2012, 2016)
# the indicator is fetched by fuzzy look up
# the start and end dates are optional
# the indicator returned is the UIS indicator ID or optionally the short key
# or full key
# Metadata is returned by default



"""