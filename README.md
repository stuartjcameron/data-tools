# data-tools

Unofficial python modules for accessing the [UNESCO Institute for Statistics](http://uis.unesco.org/)
 [API](https://apiportal.uis.unesco.org/)
 to get international education statistics.


Example usage (also see uis_api-usage-eg.py)

1. Initialize the API
```
import uis
api = uis.Api(subscription_key="your-key-here")
#api.verification = False   # NOT RECOMMENDED but sometimes need to do this to make it work
```

2. Get the response from the server for a particular UIS indicator
```
response = api.query("nara.1")   # we use the UIS indicator ID
print(response.response.url)   # shows the URL that was queried
print(response.response.text)   # the raw response from the server 
print(response.get_arranged_json(metadata=None))  # a more useful JSON format
```

3. Get disaggregated data for some countries
```
countries = ['AF', 'AL', 'BD', 'BJ', 'BT', 'BF', 'BI', 'CV', 'KH', 'CM', 'CF', 'TD', 'KM', 'CD', 'CG', 'CI', 'DJ', 'DM', 'ER', 'ET', 'GM', 'GE', 'GH', 'GD', 'GN', 'GW', 'GY', 'HT', 'HN', 'KE', 'KI', 'KG', 'LA', 'LS', 'LR', 'MG', 'MW', 'ML', 'MH', 'MR', 'FM', 'MD', 'MN', 'MZ', 'MM', 'NP', 'NI', 'NE', 'NG', 'PK', 'PG', 'RW', 'LC', 'VC', 'ST', 'SN', 'SL', 'SO', 'SS', 'SD', 'TJ', 'TZ', 'TL', 'TG', 'UG', 'UZ', 'VU', 'VN', 'YE', 'ZM', 'ZW', 'TO', 'TV', 'WS', 'SB']
out_of_school = api.query("ROFST.1.cp", by="sex", countries=countries)
print(out_of_school['ROFST.1.F.cp']["BD"])  # Female attendance rates in Bangladesh
print(out_of_school["metadata"]["indicators"])      # Indicator metadata
latest = uis.latest_by_country(out_of_school)
print(latest[latest["REF_AREA"] == "TZ"][["TIME_PERIOD", "SEX", "Value"]])

```
4. Use fuzzy lookup to explore what indicators are available
```
I = uis.Indicator
print(I.fuzzy_lookup("out of school"))  # get the main indicators on rate of out of school
print(I.fuzzy_lookup("out of school number")) # numbers OOS instead of rates
print(I.fuzzy_lookup("out of school", by="sex")) 
print(I.fuzzy_lookup("rofst.1.cp", uis.Indicator.SUB))  # include 'child' indicators 
print(I.fuzzy_lookup("rofst.1.cp", uis.Indicator.ALL))  # include all related indicators
```

5. Find some indicators and then filter the data and make a bar plot
```
# wealth_quintile is only available for household survey based indicators
s = uis.Indicator.fuzzy_lookup("primary out of school", by="wealth_quintile")

# List the full labels
for indicator in s:
    print("Indicator {}: {}".format(indicator.id, indicator.label))

# Now look these up for Tanzania and Kenya
oos_by_wealth = api.query(s, country=["Tanzania", "Kenya"]).dataframe
latest = uis.latest_by_country(oos_by_wealth)

# Plot the latest for Tanzania
tz = latest.query('REF_AREA == "TZ" and WEALTH_QUINTILE != "_T"')
print("Data on {Indicator Label - EN} for {UN country name} ({Region}), {TIME_PERIOD}".format(**tz.iloc[0]))
tz.plot.bar(x="WEALTH_QUINTILE", y="Value")

# Plot Tanzania and Kenya
table = latest[latest["WEALTH_QUINTILE"] != "_T"].pivot(index="WEALTH_QUINTILE", columns="UN country name", values="Value")
table.plot.bar()
```

## Notes

These modules are provided with no guarantees and the author is not associated
with UNESCO Institute for Statistics.

The modules use the [HDX Python Country Library](https://pypi.org/project/hdx-python-country/)
to look up country names. It uses fuzzy look-up so "Tanzania", "TZ" etc. are acceptable.

The file `input-data\combined indicators.csv` lists all of the indicators 
in the UIS education statistics dataflow. The modules allow indicators to be
referred to either by their Indicator ID (ROFST.1.cp), their "key" (e.g. ROFST.PT.L1._T._T.SCH_AGE_GROUP._T.INST_T._Z._Z._T._T._T._Z._Z._Z._Z._Z.W00.W00._Z
) or by a shortened key that can be found in the CSV file (e.g. rofst-1).


