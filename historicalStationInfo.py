import parseLiveCitibikeData as parse
import citibikeStationLocation as location

def getStationDataForTime(time, directory):
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
  from time in range(from_time, to_time, increments):
    rtn[time] = getStationDataForTime(time, directory)
  return rtn
