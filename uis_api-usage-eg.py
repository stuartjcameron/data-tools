# -*- coding: utf-8 -*-
"""
Created on Mon Jun  3 09:23:07 2019

Example: Get adjusted net enrolment rate for Bangladesh and Uganda,
2012-16.
@author: WB390262
"""
import uis
from icy_sdmx import sdmx_to_icy
#cache = Cache("C:/Users/wb390262/Documents/Miscpy/json")

#inds = uis.Indicator.match(stat_unit="ASER")
ind2 = uis.Indicator.fuzzy_lookup("rofst 3 f rur")
#inds3 = uis.Indicator.fuzzy_lookup("rofst 3 f rur", shortest=False)
api = uis.Api("8be270194d6444189bdde1a7b2666911")
api.verification = False

#my_data = api.qquery("rofst 3 f rur", ["Bangladesh", "Uganda", "India"], 2000, 2018)
bigger_data = api.qquery("rofst 3 f rur")
raw_message = api.get(ind2.spec,  {"dimension_at_observation": "AllDimensions"})
icy = sdmx_to_icy(raw_message)

