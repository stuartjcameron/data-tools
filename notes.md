# Notes

## Notes on sdmx_api.py
`dimension_at_observation="AllDimensions"` is usually needed but
not set by default. Consider adding this as a default.

The Filter class was used for making a dictionary of indicators
but not used at all in basic API queries. The API uses it for very basic
functions. Consider simplifying.

`time_period` is never passed in an API query! start and end
period are passed instead. time_period is only used in parsing the response,
and can be found in the response message.

However, it is still necessary to filter out `time_period` from the list of
dimensions occasionally, because if we find out the available dimension ids
by querying the API (rather than hard-coding them), time_period will be included
(with nothing to indicator it is not usually passed directly...)