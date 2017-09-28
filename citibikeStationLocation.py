import json
import urllib.request

# Returns a dictionary mapping a station id strings to {"lat": number, "lon": number}
def getStationLocations():
  url_string = "https://gbfs.citibikenyc.com/gbfs/en/station_information.json"
  url = urllib.request.urlopen(url_string)
  locations = {}
  for station in json.loads(url.read().decode())['data']['stations']:
    locations[station['station_id']] = {'lat': station['lat'],
                                        'lon': station['lon']}
  return locations
