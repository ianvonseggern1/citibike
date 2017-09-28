import parseLiveCitibikeData as parse
import citibikeStationLocation as location

def getStationDataForTime(time):
  data = parse.readFromFileToDictionary(parse.filePathForTime(time))
  locations = location.getStationLocations()
  for station_id in data:
    if station_id in locations:
      data[station_id]['lat'] = locations[station_id]['lat']
      data[station_id]['lon'] = locations[station_id]['lon']
  return data
