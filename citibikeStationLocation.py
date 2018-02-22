import json
import urllib.request
import pickle
import os

CACHE_DIRECTORY = './citibike_availability_data/'
CACHE_FILE = 'station_locations'

# Returns a dictionary mapping a station id strings to {"lat": number, "lon": number}
def downloadStationLocations():
  url_string = "https://gbfs.citibikenyc.com/gbfs/en/station_information.json"
  url = urllib.request.urlopen(url_string)
  locations = {}
  for station in json.loads(url.read().decode())['data']['stations']:
    locations[station['station_id']] = {'lat': station['lat'],
                                        'lon': station['lon'],
                                        'name': station['name']}
  return locations

# First checks to see if we have cached station locations. If not it downloads them
# and caches them
def getStationLocations():
  locations = loadStationLocations()
  if locations is not None:
    return locations

  print("Unable to find a cache of station locations. Downloading instead.")
  locations = downloadStationLocations()

  try:
    saveStationLocations(locations)
  except:
    print("Unable to cache stations locations.")

  return locations

def saveStationLocations(locations):
  path = CACHE_DIRECTORY
  if not os.path.exists(path):
    os.makedirs(path)
  path += CACHE_FILE
  with open(path, 'wb') as f:
    pickle.dump(locations, f)
  
def loadStationLocations():
  try:
    with open(CACHE_DIRECTORY + CACHE_FILE, 'rb') as f:
      return pickle.load(f)
  except:
    return None

# This redownloads the locations and names. This lets the script be
# chron-ed
if __name__ == "__main__":
  locations = downloadStationLocations()
  if len(locations) > 0:
    saveStationLocations(locations)
