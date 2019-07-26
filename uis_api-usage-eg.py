# -*- coding: utf-8 -*-
"""
Some examples of using the SDMX API for accessing the UNESCO
Institute for Statistics database to get education data.

@author: https://github.com/stuartjcameron
"""

import uis
# 1. Initialize the API
#subscription_key = "your-key-here"
api = uis.Api(subscription_key)
api.verification = False   # not secure but sometimes needed...

# 2. Get data by UIS key using quick_query
response = api.query("nara.1") 
print(response.response.url)   # shows the URL that was queried
print(response.response.text)   # the raw response from the server 

# Arrange the data into a more useful json format
print(response.get_arranged_json(metadata=None)) 

# 3. Get disaggregated data in JSON format
disag_nara = api.query("nara.1", by="sex").get_arranged_json()
print(disag_nara['NARA.1.F']["TZ"])  # Female attendance rates in Tanzania
print(disag_nara["metadata"]["indicators"])      # Indicator metadata

# 4. Specify some countries and get the result as a Pandas dataframe
countries = ['AF', 'AL', 'BD', 'BJ', 'BT', 'BF', 'BI', 'CV', 'KH', 'CM', 'CF', 'TD', 'KM', 'CD', 'CG', 'CI', 'DJ', 'DM', 'ER', 'ET', 'GM', 'GE', 'GH', 'GD', 'GN', 'GW', 'GY', 'HT', 'HN', 'KE', 'KI', 'KG', 'LA', 'LS', 'LR', 'MG', 'MW', 'ML', 'MH', 'MR', 'FM', 'MD', 'MN', 'MZ', 'MM', 'NP', 'NI', 'NE', 'NG', 'PK', 'PG', 'RW', 'LC', 'VC', 'ST', 'SN', 'SL', 'SO', 'SS', 'SD', 'TJ', 'TZ', 'TL', 'TG', 'UG', 'UZ', 'VU', 'VN', 'YE', 'ZM', 'ZW', 'TO', 'TV', 'WS', 'SB']
out_of_school = api.query("ROFST.1.cp", by="sex", country=countries).dataframe
latest = uis.latest_by_country(out_of_school)
print(latest[latest["REF_AREA"] == "TZ"][["TIME_PERIOD", "SEX", "Value"]])

# 5. Use fuzzy lookup to explore what indicators are available
I = uis.Indicator
print(I.fuzzy_lookup("out of school"))  # get the main indicators on rate of out of school
print(I.fuzzy_lookup("out of school number")) # numbers OOS instead of rates
print(I.fuzzy_lookup("out of school", by="sex")) 
print(I.fuzzy_lookup("rofst.1.cp", uis.Indicator.SUB))  # include 'child' indicators 
print(I.fuzzy_lookup("rofst.1.cp", uis.Indicator.ALL))  # include all related indicators

# wealth_quintile is only available for household survey based indicators
s = uis.Indicator.fuzzy_lookup("primary out of school", by="wealth_quintile")

# List the full labels
for indicator in s:
    print("Indicator {}: {}".format(indicator.id, indicator.label))

# Now look these up 
oos_by_wealth = api.query(s, country=["Tanzania", "Kenya"]).dataframe
latest = uis.latest_by_country(oos_by_wealth)

# Plot the latest for Tanzania
tz = latest.query('REF_AREA == "TZ" and WEALTH_QUINTILE != "_T"')
print("Data on {Indicator Label - EN} for {UN country name} ({Region}), {TIME_PERIOD}".format(**tz.iloc[0]))
tz.plot.bar(x="WEALTH_QUINTILE", y="Value")

# Plot Tanzania and Kenya
table = latest[latest["WEALTH_QUINTILE"] != "_T"].pivot(index="WEALTH_QUINTILE", columns="UN country name", values="Value")
table.plot.bar()