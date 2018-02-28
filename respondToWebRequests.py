import json
import datetime
from urllib.parse import urlparse, parse_qs
import reindexByStation as stationIndex
import citibikeStationLocation as stationLocations

# Parses the query parameters and creates the JSON response to an
# http request for citibike summary information.
# Response is a tuple of first content type and then the json data
# encoded in bytes
def doCitibikeSummaryJson(request):
    parsed_url = urlparse(request)
    query_parameters = parse_qs(parsed_url.query)

    start_time = 360
    end_time = 1440
    increment = 60
    station_type = 'bike'
    station = None
    start_date = None
    end_date = None
    if 'start_time' in query_parameters:
        start_time = int(query_parameters['start_time'][0])
    if 'end_time' in query_parameters:
        end_time = int(query_parameters['end_time'][0])
    if 'step' in query_parameters:
        increment = int(query_parameters['step'][0])
    if 'station_type' in query_parameters and query_parameters['station_type'][0] == 'dock':
        station_type = 'dock'
    if 'station' in query_parameters:
        station = query_parameters['station'][0]
    if 'start_date' in query_parameters:
        start_date = datetime.datetime.strptime(query_parameters['start_date'][0], '%Y-%m-%d').date()
    if 'end_date' in query_parameters:
        end_date = datetime.datetime.strptime(query_parameters['end_date'][0], '%Y-%m-%d').date()

    if start_date is None or end_date is None or station is None:
        return ('application/json', bytes('', 'utf8'))

    data = stationIndex.stationSummary(station, station_type == 'bike', start_date, end_date, start_time, end_time, increment)
    stationInfo = stationLocations.loadStationLocations()[station]
    rtn = {
        'data': data,
        'stationInfo': stationInfo,
    }
    return ('application/json', bytes(json.dumps(rtn), 'utf8'))
