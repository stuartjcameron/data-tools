# -*- coding: utf-8 -*-
"""
Created on Mon Jun  3 09:23:07 2019

Example: Get adjusted net enrolment rate for Bangladesh and Uganda,
2012-16.
@author: WB390262
"""

import uis
import translate_sdmx

#cache = Cache("C:/Users/wb390262/Documents/Miscpy/json")

#inds = uis.Indicator.match(stat_unit="ASER")

#inds3 = uis.Indicator.fuzzy_lookup("rofst 3 f rur", shortest=False)

api = uis.Api(subscription_key="8be270194d6444189bdde1a7b2666911")
api.verification = False
gpe_codes = ['AF', 'AL', 'BD', 'BJ', 'BT', 'BF', 'BI', 'CV', 'KH', 'CM', 'CF', 'TD', 'KM', 'CD', 'CG', 'CI', 'DJ', 'DM', 'ER', 'ET', 'GM', 'GE', 'GH', 'GD', 'GN', 'GW', 'GY', 'HT', 'HN', 'KE', 'KI', 'KG', 'LA', 'LS', 'LR', 'MG', 'MW', 'ML', 'MH', 'MR', 'FM', 'MD', 'MN', 'MZ', 'MM', 'NP', 'NI', 'NE', 'NG', 'PK', 'PG', 'RW', 'LC', 'VC', 'ST', 'SN', 'SL', 'SO', 'SS', 'SD', 'TJ', 'TZ', 'TL', 'TG', 'UG', 'UZ', 'VU', 'VN', 'YE', 'ZM', 'ZW', 'TO', 'TV', 'WS', 'SB']

#my_data = api.qquery("rofst 3 f rur", ["Bangladesh", "Uganda", "India"], 2000, 2018)
#my_data = api.quick_query("rofst 3 f rur", country=gpe_codes)

# You can also get the SDMX message directly like this:
ind2 = uis.Indicator.fuzzy_lookup("rofst 3 f rur")
raw_message = api.get(ind2.spec,  {"dimension_at_observation": "AllDimensions"})
df = translate_sdmx.to_df(raw_message)
df["Indicator"] = df["Indicator key"].apply(lambda k: uis.Indicator(key=k).id)

df2 = api.df_query("rofst 3 f rur")
#df2.to_csv("ROFST lower secondary rural girls.csv")

gpe_latest = uis.latest_by_country(df[df["REF_AREA"].isin(gpe_codes)])
selection = gpe_latest[gpe_latest["REF_AREA"].isin(['AF', 'AL', 'BD', 'BJ', 'BT', 'BF'])]
to_plot = selection[["REF_AREA", "Value"]].sort_values("Value", ascending=False)
to_plot.plot.barh(x="REF_AREA")

#icy = translate_sdmx.to_icy(raw_message)

#oos = api.quick_query("rofst 2", gpe_codes)

def defaultdict_to_dict(d):
    if isinstance(d, dict):
        return {k: defaultdict_to_dict(v) for k, v in d.items()}
    else:
        return d
    
"""
TODO: Consider ways of getting multiple dimensions 
e.g. 
df = api.df_query("rofst+") -- to get all indicators relating to rofst
df = api.df_query("rofst", by=["sex", "level"]) -- to get the given disaggregations only

TODO: convenient plot wrappers with common disaggregations

TODO: throw errors when:
    - indicator / country not found
    - tried to process a message but no data was returned (state what URL was queried)


Notes
2019.06.19 something seems a bit wrong with the API - no data for stat_unit
ROFST - is the rate of OOSC being stored somewhere else?!
https://api.uis.unesco.org/sdmx/data/UNESCO,EDU_NON_FINANCE,3.0/ROFST.....................?format=sdmx-compact-2.1&locale=en&subscription-key=8be270194d6444189bdde1a7b2666911

API seems unreliable today - also returning no data for ROFST_PHH sometimes

Code above is not working at present.
"""

