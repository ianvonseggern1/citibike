import parseLiveCitibikeData as parse
import citibikeStationLocation as location

import datetime

def getStationDataAndLocationsForTime(time, directory):
  data = parse.readFromFileToDictionary(directory + parse.filePathForTime(time))
  locations = location.getStationLocations()
  for station_id in data:
    if station_id in locations:
      data[station_id]['lat'] = locations[station_id]['lat']
      data[station_id]['lon'] = locations[station_id]['lon']
  return data

# from_time and to_time are minutes past midnight
# returns a dictionary mapping time (minutes past midnight) to a dictionary of
# stations to bikes
def getStationData(date, directory, from_time, to_time, increments):
  rtn = {}
  for time_minutes in range(from_time, to_time, increments):
    time_object = datetime.datetime(date.year, date.month, date.day,
                                    int(time_minutes / 60), time_minutes % 60)
    path = directory + parse.filePathForTime(time_object)

    try:
      rtn[time_minutes] = parse.readFromFileToDictionary(path)
    except:
      print("Couldn't find file at: " + path)
      
  return rtn

# Returns a dictionary of data and locations
def getStationDataAndLocations(date, directory, from_time, to_time, increments):
  return {
    'data': getStationData(date, directory, from_time, to_time, increments),
    'locations': location.getStationLocations(),
  };
  
# station is the station_id as a string
def getSpecificStationData(station, date, directory, from_time, to_time, increments):
  data = getStationData(date, directory, from_time, to_time, increments)
  return {key: value[station] for (key, value) in data.items()}
