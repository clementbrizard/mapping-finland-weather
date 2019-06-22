#%%
# Connect to keyspace
from cassandra.cluster import Cluster
cluster = Cluster()
session = cluster.connect('finland_weather_metar')
print("Connection to keyspace done")
#%%
# Create table template
from create_table_temporal import create_table_temporal
create_table_temporal(session)
print("Table creation done")

#%%
# Load data
from load_data import load_data
data = load_data("../asos.txt")
print("Data loading done")

#%%
# Insert data
from write_data import write_cassandra
write_cassandra(session, data)
print("Data insertion done")
