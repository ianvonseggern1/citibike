import datetime
import os

from collections import Counter

import bitstring

import sys
sys.path.append('/root/citibike/citibike/')
import parseLiveCitibikeData as parse

# parseLiveCitibikeData scrapes the API at the moment and can be
# used with a cron job to store historical data. Because of this it is
# indexed by time. For many uses it would be more helpful to have
# this data indexed primarily by station_id. This file contains the
# code for taking a days worth of data and reindexing.

# Minutes between scrapes. This is not used on the read side, but
# is used as an assumption on the write side
SCRAPE_INTERVAL = 2

# Each value is stored as 8 bits, any value over the max is stored
# as the max value.
# Special encodings are used to indicate something other than a count
# These special encodings are values above the max value.
NO_DATA_AVAILABLE = b'\xff'
STATION_NOT_RENTING = b'\xfe' # Iff docks, this means station not accepting returns
MAX_COUNT_VALUE = 250 # Leaving a bit of breathing room if other special values are required in the future

# Returns point data at a regular interval from one time to another on
# a specific day. Equivilant to historicalStationInfo's getSpecificStationData
# but more efficient
#
# station: string representing citibike system's station id
# date: date object for day in question
# is_bike: bool, true for bike data, false for dock data
# from_time: minutes past midnight
# to_time: minutes past midnight
# increment: minutes
#
# Returns dictionary key'ed on time since midnight, value is a dict of
# data_available: bool, is_renting: bool, count: int
def getStationData(station, date, is_bike, from_time, to_time, increment, directory = '/root/'):
    if(from_time % SCRAPE_INTERVAL != 0 or to_time % SCRAPE_INTERVAL != 0 or increment % SCRAPE_INTERVAL != 0):
        print('Times must be a multiple of the SCRAPE_INTERVAL')

    rtn = {}
    path = directory + getFilepath(date, is_bike, station)
    file_contents = open(path, 'rb')
    for time in range(from_time, to_time, increment):
        position = int(time / SCRAPE_INTERVAL)
        file_contents.seek(position)
        value = file_contents.read(1)
        if value == NO_DATA_AVAILABLE:
            rtn[time] = {'data_available': False}
        elif value == STATION_NOT_RENTING:
            rtn[time] = {'data_available': True, 'is_renting': False}
        else:
            count = int.from_bytes(value, byteorder='little')
            rtn[time] = {'data_available': True, 'is_renting': True, 'count': count}
    return rtn

# Provides a summary by giving a dictionary of samples from a time increment
# with a count of empty stations, low stations (1-2), and stations with available
# bikes/docks (3+). Also has a count of data not available and not renting.
# Data is returned in nested arrays, one array per day, each of those holding one
# dictionary for each increment of time from from_time to to_time.
def stationSummary(station, is_bike, from_date, to_date, from_time, to_time, increment, directory='/root/'):
    if(from_time % SCRAPE_INTERVAL != 0 or to_time % SCRAPE_INTERVAL != 0 or increment % SCRAPE_INTERVAL != 0):
        print('Times must be a multiple of the SCRAPE_INTERVAL')

    dates = []
    date = from_date
    while date <= to_date:
        dates.append(date)
        date += datetime.timedelta(days=1)

    return [stationSummaryForDay(station, is_bike, date, from_time, to_time, increment, directory) for date in dates]

def stationSummaryForDay(station, is_bike, date, from_time, to_time, increment, directory):
    rtn = []
    path = directory + getFilepath(date, is_bike, station)
    file_contents = open(path, 'rb')
    file_start_position = int(from_time / SCRAPE_INTERVAL)
    file_contents.seek(file_start_position)
    samples = int(increment / SCRAPE_INTERVAL)
    for time in range(from_time, to_time, increment):
        counter = Counter()
        for sample in range(samples):
            value = file_contents.read(1)
            if value == NO_DATA_AVAILABLE:
                counter['data_not_available'] += 1
            elif value == STATION_NOT_RENTING:
                counter['not_renting'] += 1
            else:
                count = int.from_bytes(value, byteorder='little')
                if count == 0:
                    counter['empty'] += 1
                elif count < 3:
                    counter['low'] += 1
                else:
                    counter['available'] += 1
        rtn.append(dict(counter))
    return rtn

# Assumes data exists on a SCRAPE_INTERVAL basis
def writeDayOfDataByStation(day, print_progress=True):
    data_indexed_by_station = getDayOfData(day)

    total_stations = len(data_indexed_by_station.keys())
    stations_processed = 0
    
    for station_id in data_indexed_by_station.keys():
        station_data = data_indexed_by_station[station_id]
        
        bikes_path = getFilepath(day, True, station_id)
        bikes_file = createDirectoryOpenFile(bikes_path)
        docks_path = getFilepath(day, False, station_id)
        docks_file = createDirectoryOpenFile(docks_path)

        # time is minutes past midnight
        for time in range(0, 24*60, SCRAPE_INTERVAL):
            if not time in station_data:
                bikes_file.write(NO_DATA_AVAILABLE)
                docks_file.write(NO_DATA_AVAILABLE)
                continue
            
            if not station_data[time]['is_renting']:
                bikes_file.write(STATION_NOT_RENTING)
            else:
                bikes_string = bitstring.pack('uint:8', station_data[time]['bikes']).bytes
                bikes_file.write(bikes_string)

            if not station_data[time]['is_returning']:
                docks_file.write(STATION_NOT_RENTING)
            else:
                docks_string = bitstring.pack('uint:8', station_data[time]['docks']).bytes
                docks_file.write(docks_string)

        docks_file.close()
        bikes_file.close()

        stations_processed += 1
        if print_progress and stations_processed % int(total_stations / 10) == 0:
            print(str(int(100 * stations_processed / total_stations)) + '%')

# Inclusive of both endpoints
def writeBatchOfData(from_day, to_day):
    day = from_day
    while day <= to_day:
        writeDayOfDataByStation(day, False)
        print('Finished reindexing ' + str(day))
        day += datetime.timedelta(days=1)

def createDirectoryOpenFile(path):
    directory = os.path.dirname(path)
    if not os.path.exists(directory):
        os.makedirs(directory)
    return open(path, 'wb')
        
# day: A date object
# is_bike: iff true its bikes available, otherwise docks
# station_id: the id given to the station by citibike
def getFilepath(day, is_bike, station_id):
    path = parse.BASE_PATH + 'by_station/'
    path += 'bikes/' if is_bike else 'docks/'
    path += station_id + '/'
    path += str(day.year) + '/'
    path += str(day.month) + '/'
    path += str(day.day) + '.data'
    return path
    

# This reads in a days worth of data and reindexs it by station_id
# it returns {[station_id]: {[minutes_past_midnight]: [station_data]}}
def getDayOfData(day):
    # Get day's data, store in a dictionary indexed by station
    station_to_data = {}

    midnight = datetime.datetime(day.year, day.month, day.day)
    midnight_path = parse.filePathForTime(midnight)
    # We can get the path to today's directory by stripping the hh/mm.data
    day_path = '/'.join(midnight_path.split('/')[:5])

    for (directory_path, _, filenames) in os.walk(day_path):
        for filename in filenames:            
            data = parse.readFromFileToDictionary(directory_path + '/' + filename)
            hour = int(directory_path.split('/')[-1])
            minute = int(filename.split('.')[0])
            minutes_past_midnight = hour * 60 + minute
            for station_id in data.keys():
                station_data = data[station_id]
                if station_id in station_to_data:
                    station_to_data[station_id][minutes_past_midnight] = station_data
                else:
                    station_to_data[station_id] = {minutes_past_midnight: station_data}
    return station_to_data
                
# By default we write yesterday. This way we can set up a cron job for it
if __name__ == "__main__":
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    writeDayOfDataByStation(yesterday)
            
