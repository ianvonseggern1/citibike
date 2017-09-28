import json
import urllib.request
import datetime
import os

import bitstring

# Provides an array of dictionaries for all stations giving station_id
# as well as num_bikes_available and num_docks_available. Also an is_renting
# and is_returning bool
def downloadCurrentCitibikeData():
  url_string = "https://gbfs.citibikenyc.com/gbfs/en/station_status.json"
  url = urllib.request.urlopen(url_string)
  return json.loads(url.read().decode())['data']['stations']
  
# This packs information about a single station into 4 bytes
# The scheme:
# 14 bits for station_id
# 1 bit for is_renting
# 8 bits for num_bike_available
# 1 bit for is_returning
# 8 bits for num_docks_available
def packStationData(station):
  return bitstring.pack('uint:14, uint:1,  uint:8, uint:1, uint:8',
                        int(station['station_id']),
                        station['is_renting'],
                        station['num_bikes_available'],
                        station['is_returning'],
                        station['num_docks_available']).bytes

def unpackStationData(packed):
  data_array = bitstring.BitStream(bytes=packed).unpack('uint:14, uint:1,  uint:8, uint:1, uint:8')
  return {'station_id': str(data_array[0]),
          'is_renting': data_array[1],
          'num_bikes_available': data_array[2],
          'is_returning': data_array[3],
          'num_docks_available': data_array[4]}

def writeToFile(filename):
  stations = downloadCurrentCitibikeData()
  file = open(filename, 'wb')
  for station in stations:
    file.write(packStationData(station))
  file.close()

def readFromFile(filename):
  stations = []
  file = open(filename, 'rb')
  bytes = file.read(4)
  while len(bytes) == 4:
    stations.append(unpackStationData(bytes))
    bytes = file.read(4)
  return stations

def readFromFileToDictionary(filename):
  stations = {}
  file = open(filename, 'rb')
  bytes = file.read(4)
  while len(bytes) == 4:
    station_data = unpackStationData(bytes)
    station_id = station_data.pop('station_id')
    stations[station_id] = station_data

    bytes = file.read(4)
  return stations

# Creates a filepath of yyyy/mm/dd/hh/mm.data
def filePathForTime(time):
  path = './citibike_availability_data/'
  path += str(time.year) + '/'
  path += str(time.month) + '/'
  path += str(time.day) + '/'
  path += str(time.hour) + '/'
  path += str(time.minute) + '.data'
  return path

if __name__ == "__main__":
  path = filePathForTime(datetime.datetime.now())
  directory_path = os.path.dirname(path)
  if not os.path.exists(directory_path):
    os.makedirs(directory_path)
  writeToFile(path)
