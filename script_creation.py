#! /usr/bin/python3


from cassandra.cluster import Cluster

cluster = Cluster()

session = cluster.connect('e04')


# Clean the tables

session.execute("""
DROP TABLE IF EXISTS table_faits_time;
""")

session.execute("""
DROP TABLE IF EXISTS table_faits_location;
""")

session.execute("""
DROP TABLE IF EXISTS table_faits_location;
""")


# Then create the tables

session.execute("""
CREATE TABLE IF NOT EXISTS table_faits_time (trip_id double, taxi_id double, year int, month int, day int,
hour int, min int, daytype varchar, starting_point varchar,
ending_point varchar, dist double, call_type varchar, origin_stand double,
origin_call double, primary key((year, month, day), hour, min, trip_id))
""")

session.execute("""
CREATE TABLE IF NOT EXISTS table_faits_location (trip_id double, taxi_id double, year int, month int, day int,
hour int, min int, daytype varchar, starting_point varchar,
ending_point varchar, start_cluster_id int, dist double, call_type varchar, origin_stand double,
origin_call double, primary key((year, month, day), hour, min, trip_id))
""")

session.execute("""
CREATE TABLE IF NOT EXISTS table_clusters (cluster_id int, cluster_position varchar, primary key(cluster_id, cluster_position))
""")
