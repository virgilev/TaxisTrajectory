#! /usr/bin/python3

import csv
import json
import arrow


from cassandra.cluster import Cluster
from math import *
import csv
import datetime
import pandas as pd
import json
import arrow

import script_k_means


cluster = Cluster()
session = cluster.connect('e04')

session.default_timeout = 9999


# Calcul de distance

def dist(lon1, lon2, lat1, lat2):
	import numpy as np
	RT = 6371008
	d = np.sqrt(
	   ((lon1-lon2)*np.cos((lat1+lat2)/2/180*np.pi))**2
	)/180*np.pi*RT
	return d

def get_closest_cluster_id(lat, lon):
	res = 0
	i = 0
	current_distance = 9999999999999999999999999
	for center in centers
		dist_point_center = dist(lon, center[0], lat, center[1])
        if dist_point_center < current_distance:
            res = centers.index(center)
            current_distance = dist_point_center
		i = i + 1
		return res



# Insert tables

def insert(table_name):
	with open('../../train.csv') as f:
		nbligne = 0
		l=f.readline()
		while True:
			l=f.readline()
			nbligne += 1
			if len(l)==0:
				break
			data = l.split("\",\"")
			id_trip = data[0][1:]
			call_type = data[1]
			origin_call = data[2]
			origin_stand = data[3]
			id_taxi = data[4]
			timestamp = data[5]
			day_type = data[6]
			data_missing = data[7]
			chemin = data[8][2:-4] # "[[X,Y],[X,Y]]"
			positions = chemin.split("],[")
			
			date = datetime.datetime.fromtimestamp(int(timestamp))
			year = date.year
			month = date.month
			day = date.day
			hour = date.hour
			minute = date.minute

			
			Bdays = ['112013','2932013','3132013','2542013','152013','1062013','1582013','8122013','25122013','112014','1842014','2042014','2542014','152014','1062014','1582014','8122014','25122014']
			Cdays = ['31122012','2832013','3032013','2442013','3042013','962013','1482013','7122013','24122013','31122013','1742014','1942014','2442014','3042014','962014','1482014','7122014','24122014']

			daymonthyear = str(day) + str(month) + str(year)

			if daymonthyear in Bdays:
				day_type='B'

			if daymonthyear in Cdays:
				day_type='C'

			d_requete = "INSERT INTO e04." + table_name + " (trip_id, taxi_id"
			f_requete = "VALUES (%s, %s" % (id_trip, id_taxi)

			d_requete += ", year, month, day, hour, min, daytype"
			f_requete += ", %s, %s, %s, %s, %s, '%s'" % (year, month, day, hour, minute, day_type)

			d_requete += ", call_type"
			f_requete += ", '%s'" % (call_type)

			if(call_type == "A"):
				if call_type:
					d_requete += ", origin_call"
					f_requete += ", %s" %(origin_call)
				else:
					continue

			elif(call_type == "B"):
				if origin_stand:
					d_requete += ", origin_stand"
					f_requete += ", %s" %(origin_stand)
				else:
					continue

			if (len(positions)>=2):
				lon1= float(positions[0].split(",")[0])
				lat1= float(positions[0].split(",")[1])
				lon2= float(positions[-1].split(",")[0])
				lat2= float(positions[-1].split(",")[1])
				distance = dist(lon1,lat1,lon2,lat2)
				lon1= "%.2f" % float(positions[0].split(",")[0])
				lat1= "%.2f" % float(positions[0].split(",")[1])
				lon2= "%.2f" % float(positions[-1].split(",")[0])
				lat2= "%.2f" % float(positions[-1].split(",")[1])

				d_requete+= ", starting_point, ending_point, dist"
				f_requete+= ", '[%s,%s]','[%s,%s]', %s" % (lon1,lat1,lon2,lat2,distance)

				if(table_name=="table_faits_location")
					start_cluster_id = get_closest_cluster_id(lon1, lat1)
					d_requete+= ", start_cluster_id"
					f_requete+= ", %s" % (start_cluster_id)

			else:
				continue


			d_requete += ") "
			f_requete += ");"

			session.execute(d_requete+f_requete)
			print(nbligne)



def insert():
	for i in range(1, 10)
		requete = "INSERT INTO e04.table_clusters (cluster_id, cluster_position)"
		requete += "VALUES (%s, %s" % (i, centers[i-1]) +");"

		session.execute(requete)





insert("table_faits_time")

global centers
centers = script_k_means.compute_kmeans(9)

insert_k_means()

insert("table_faits_location")
