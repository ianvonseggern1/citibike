import psycopg2
from datetime import datetime
import os
import pytz

import parseLiveCitibikeData as p

# Assumes you have postgres in a docker container with the username, dbname and password below
#
# table is called citibike
# # \d citibike
# Table "public.citibike"
#      Column      |            Type             | Collation | Nullable | Default 
# -----------------+-----------------------------+-----------+----------+---------
#  station_id      | integer                     |           |          | 
#  bikes_available | integer                     |           |          | 
#  docks_available | integer                     |           |          | 
#  is_renting      | boolean                     |           |          | 
#  is_returning    | boolean                     |           |          | 
#  time            | timestamp without time zone |           |          | 
#
# data is a json mapping station id to the following value
# {'is_renting': 1, 'num_bikes_available': 13, 'is_returning': 1, 'num_docks_available': 25}
def insertData(data, timestamp):
    eastern_time = pytz.timezone('US/Eastern').localize(timestamp)
    utc_time = eastern_time.astimezone(pytz.utc)
    utc_timestamp = utc_time.strftime("%Y-%m-%d %H:%M:%S")

    conn = psycopg2.connect("dbname='postgres' user='postgres' host='localhost' password='docker'")
    c = conn.cursor()
    values = ["({}, {}, {}, {}, {}, {})".format(
        station_id,
        vals["bikes"],
        vals["docks"],
        vals["is_renting"],
        vals["is_returning"],
        utc_timestamp) for station_id, vals in data.items()]
    cur.execute("INSERT INTO citibike VALUES {}".format(values.join(", ")))

# Assumes relative file paths are of the form yyyy/mm/dd/hh/mm.data
# Walks all files in the directory and inserts the values into the DB
def walkFiles(directory):
    for current_dir, sub_dir, files in os.walk(directory):
        if len(files) > 0:
            print("Adding {}".format(current_dir))
        for f in files:
            path = os.path.join(current_dir, f)
            data = p.readFromFileToDictionary(path)
            # Path is of the form ./yyyy/mm/dd/hh/mm.data (in nyc timezone)
            parts = path.split("/")
            if len(parts) != 6:
                print("ERROR path {} not of length 6".format(path))
                continue
            minutes = parts[5].split(".data")[0]
            t = datetime(parts[1], parts[2], parts[3], parts[4], minutes)
            insertData(data, t)