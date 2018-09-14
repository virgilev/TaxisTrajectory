#! /usr/bin/python3

import csv
import json
import arrow


from cassandra.cluster import Cluster
from math import *

cluster = Cluster()
session = cluster.connect('e04')



trip_id = set()
call_type = set()
origin_call = set()
origin_stand = set()
taxi_id = set()
timestamp = set()
day_type = set()
missing_data = set()

year = set()
month = set()
day = set()
hour = set()
minute = set()
second = set()

start_point_lat = set()
start_point_long = set()
end_point_lat = set()
end_point_long = set()

start_pave_lat = set()
start_pave_long = set()
end_pave_lat = set()
end_pave_long = set()



with open('../../train.csv') as trainFile:
	reader = csv.reader(trainFile)
	next(reader, None)
	for row in reader:
		trip_id.add(row[0])
		call_type.add(row[1])
		origin_call.add(row[2])
		origin_stand.add(row[3])
		taxi_id.add(row[4])
		# timestamp.add(row[5])
		day_type.add(row[6])
		missing_data.add(row[7])
		polyline = json.loads(row[8])

		date = arrow.Arrow.utcfromtimestamp(row[5])

		year.add(date.year)
		month.add(date.month)
		day.add(date.day)
		hour.add(date.hour)
		minute.add(date.minute)
		second.add(date.second)

		if(polyline.size() == 0):
			start_point_lat = null
			end_point_long = null
            start_pave_lat = null
            start_pave_long = null
            end_pave_lat = null
            end_pave_long = null
		else:
			start_point_long,start_point_lat = polyline[0].split(",")
			polyline = polyline.reverse()
			end_point_long,end_point_lat = polyline[0].split(",")
            
            start_pave_lat = floor(start_point_lat*1000)/1000
            start_pave_long = floor(start_point_long*1000)/1000
            end_pave_lat = floor(end_point_lat*1000)/1000
            end_pave_long = floor(end_point_long*1000)/1000

		#print(date.year, date.month, date.day)

	print("fin")




CREATE TABLE faits1 (
	trip_id int,
	call_type char(1),
	origin_call int,
	origin_stand int,
	taxi_id int,
	missing_data bool,
	day_type char(1),
	year int,
	month int,
	day int,
	hour int,
	minute int,
	second int,
	start_point float,
	end_point float,
	start_pave_lat float,
	start_pave_long float,
    end_pave_lat float,
	end_pave_long float,
	PRIMARY KEY ((year, month, day), hour, minute, second, trip_id)
);


CREATE TABLE faits2 (
	trip_id int,
	call_type char(1),
	origin_call int,
	origin_stand int,
	taxi_id int,
	missing_data bool,
	day_type char(1),
	year int,
	month int,
	day int,
	hour int,
	minute int,
	second int,
	start_point float,
	end_point float,
	start_pave_lat float,
	start_pave_long float,
    end_pave_lat float,
	end_pave_long float,
	PRIMARY KEY ((start_pave, year), month, day, hour, minute, second, trip_id)
);



