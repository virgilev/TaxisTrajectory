#! /usr/bin/python3


import matplotlib.pyplot as plt
import numpy as np
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

'''
umap.openstreetmap.fr
plt.plot(x,y,'v')
plt.show()
plt.savefig()

result = session.execute(select...) retourne un iterable
-> for row in result

j le num de centroid
    s[j]+=x
    n[j]+=1

C=s/n


1) Randomly select ‘c’ cluster centers.

2) Calculate the distance between each data point and cluster centers.

3) Assign the data point to the cluster center whose distance from the cluster center is minimum of all the cluster centers..

4) Recalculate the new cluster center

5) Recalculate the distance between each data point and new obtained cluster centers.

6) If no data point was reassigned then stop, otherwise repeat from step 3).


New center cluster 2 --> n2 * center2 + point / n2 + 1
New center cluster 1 --> n1 * center1 - point / n1 - 1
'''




def dist(lon1, lon2, lat1, lat2):
	import numpy as np
	RT = 6371008
	d = np.sqrt(
	   ((lon1-lon2)*np.cos((lat1+lat2)/2/180*np.pi))**2
	)/180*np.pi*RT
	return d



def select_initial_centers(number):
    global centers
    centers = [0] * number
    centers_string = [""] * number
    for n in range(number):
        min = 1
        point = "[00.00,00.00]"
        rows = session.execute("SELECT starting_point FROM e04.table_faits_time;")
        for row in rows:
            rand = np.random.uniform(0,1)
            if rand < min:
                if row.starting_point not in centers_string:
                    point = row.starting_point
                    min = rand
        coordinates = point.split("[")[1]
        coordinates = coordinates.split("]")[0]
        lat, lon = coordinates.split(",")
        centers_string[n] = [lat,lon]
        lat = float(lat)
        lon = float(lon)
        centers[n] = [lat,lon]

    return centers




def assign_point_and_recalculate_centers(point):
    global labels
    global centers
    global center_cards
    global table_iter
    current_distance = 9999999999999999999999999
    # Find which cluster the points belong to
    cluster = labels[row_number]
    # Find closest center
    for center in centers:
        dist_point_center = dist(point[0],center[0],point[1],center[1])
        if dist_point_center < current_distance:
            cluster = centers.index(center)
            current_distance = dist_point_center
    # If closest center changed
    if cluster != labels[row_number]:
        old_cluster = labels[row_number]
        new_cluster = cluster
        # Change label
        labels[row_number] = cluster
        print(labels[row_number])
        # Recalculate new cluster center
        centers[new_cluster] = list(np.divide(np.multiply(center_cards[new_cluster], centers[new_cluster]) + point, center_cards[new_cluster] + 1))
        # Change new cluster cardinality
        center_cards[new_cluster] += 1
        if table_iter != 1:
            # Recalculate previous cluster center
            centers[old_cluster] = list(np.divide(np.multiply(center_cards[old_cluster], centers[old_cluster]) + point, center_cards[old_cluster] + 1))
            # Change previous cluster cardinality
            center_cards[old_cluster] -= 1
        # Assert that the point has switched of cluster



def compute_k_means(number):

    # List of cluster label for each table row
    global labels
    labels = [-1] * 5263
    # List of clusters centers
    global centers
    centers = kmeans_initial_centers(number)
    # List of clusters cardinalities
    global center_cards
    center_cards = [0] * number
    global table_iter
    table_iter = 0
    # While points continue to move between clusters
    for i in range(1, 6)
        table_iter += 1
        print(table_iter)
        statement = SimpleStatement("SELECT starting_point FROM e04.table_faits_time;", fetch_size=10000)
        rows = session.execute(statement)
        # Row number of table
        global row_number
        row_number = 0
        for row in rows:
            row_number += 1
            # Parse latitude and longitude of the point
            coordinates_point = row.starting_point.split("[")[1]
            coordinates_point = coordinates_point.split("]")[0]
            lat_point, lon_point = coordinates_point.split(",")
            lat_point = float(lat_point)
            lon_point = float(lon_point)
            point = [lat_point, lon_point]
            assign_point_and_recalculate_centers(point)
    # print(labels)
    return centers

    



cluster = Cluster()

session = cluster.connect('e04')
session.default_timeout = 9999

#compute_k_means(9)
#print(centers)



