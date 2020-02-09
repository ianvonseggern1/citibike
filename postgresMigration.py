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
    values = ["({}, {}, {}, {}, {}, TO_TIMESTAMP('{}','YYYY-MM-DD HH24:MI:SS'))".format(
        station_id,
        vals["bikes"],
        vals["docks"],
        'True' if vals["is_renting"] else 'False',
        'True' if vals["is_returning"] else 'False',
        utc_timestamp) for station_id, vals in data.items()]
    c.execute("INSERT INTO citibike VALUES {}".format(", ".join(values)))

# Assumes relative file paths are of the form yyyy/mm/dd/hh/mm.data
# Walks all files in the directory and inserts the values into the DB
def walkFiles(path_to_year):
    for current_dir, sub_dir, files in os.walk(path_to_year):
        if len(files) > 0:
            print("Adding month {}".format(current_dir))
        for f in files:
            path = os.path.join(path_to_year, current_dir, f)
            data = p.readFromFileToDictionary(path)
            # Path is of the form ./yyyy/mm/dd/hh/mm.data (in nyc timezone)
            parts = path.split("/")[-5:]
            minutes = parts[4].split(".data")[0]
            t = datetime(int(parts[0]), int(parts[1]), int(parts[2]), int(parts[3]), int(minutes))
            insertData(data, t)