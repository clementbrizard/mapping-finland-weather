import os
import csv
import cassandra
import numpy as np
import pandas as pd
import seaborn as sns
import mpu # to calculate distance using longitude and latitude
import matplotlib.pyplot as plt
from pyspark import SparkContext
from pyspark.sql import SQLContext


# Init environment PySpark
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.11:2.3.0 \
                                     --conf spark.cassandra.connection.host=localhost pyspark-shell'


# Get SparkContent & SQLContext
sc = SparkContext.getOrCreate()
sqlContext = SQLContext.getOrCreate(sc)


def get_table_df(keyspace_name, table_name):
    """ Get DataFrame of table in Cassandra
    """
    table_df = sqlContext.read.format("org.apache.spark.sql.cassandra") \
                         .options(keyspace=keyspace_name, table=table_name).load()
    return table_df


D = get_table_df("finland_weather_metar", "spatial").rdd


def distance_to_station(longitude, latitude):
    # Load list of stations
    with open("station_list.txt") as f:
        list_station = []
        for r in csv.DictReader(f):
            list_station.append((r['station'],float(r['longitude']),float(r['latitude'])))

    # Find station nearest distance
    min_station = (99999999, "")
    for station in list_station:
        dist = mpu.haversine_distance((latitude, longitude), (station[2], station[1]))
        if dist < min_station[0]: min_station = (dist, station[0])

    return(min_station)


def map_reduce(Data, type):
    """ Do MapReduce
    (count, mean, min, max, std)
    """
    # Map follow type
    if type == 0: # Year Month
        Tmp = Data.map(lambda d: ((d[1], d[2]), np.array([1, d[0], d[0], d[0], d[0]**2])))
    elif type == 1: # Trimester
        # Tmp = Data.map(lambda d: (((d[2]-1) // 3 + 1, d[1]), np.array([1, d[0], d[0], d[0], d[0]**2])))
        Tmp = Data.map(lambda d: ((d[2], d[1]), np.array([1, d[0], d[0], d[0], d[0]**2])))

    Tmp = Tmp.reduceByKey(lambda a, b : (a[0]+b[0], a[1]+b[1], min(a[2], b[2]), max(a[3], b[3]), a[4]+b[4]))
    Tmp = Tmp.map(lambda d: ((d[0]), d[1][1]/d[1][0], d[1][2], d[1][3], np.sqrt(-(d[1][1]/d[1][0])**2 + d[1][4]/d[1][0])))
    return Tmp


def requete_interval(station, indicateur, from_year=2005, to_year=2014):
    """ By interval of years
    """
    Spatial = D.filter(lambda d : (d['station'] == station) & (d[indicateur] is not None) & (d['year'] is not None))
    Spatial = Spatial.filter(lambda d : (from_year <= d['year'] <= to_year))
    # Map indicateurs important
    Spatial = Spatial.map(lambda d: (d[indicateur], d['year'], d['month'], d['day']))
    Data_Year = map_reduce(Spatial, 0)

    # print(Data_Year.collect())
    # return Data_Year

    Tmp = np.array(Data_Year.collect())
    Tmp = pd.DataFrame(Tmp).sort_values(by=0)
    if Tmp.empty:
        print("Empty data. No result")
        return

    Tmp.columns = ['date', 'mean', 'min', 'max', 'std']
    Tmp['month'] = Tmp['date'].apply(lambda x: int(x[1]))
    Tmp['year'] = Tmp['date'].apply(lambda x: int(x[0]))
    Tmp['date'] = Tmp['date'].apply(lambda x: pd.to_datetime(str(x[0]) + '-' + str(x[1])))
    Tmp = Tmp.set_index('date')
    # print(Tmp)

    plt.clf()
    plt.title("Need a name")
    fig = Tmp['min'].plot(linewidth=0.5, marker='', label='min').get_figure()
    fig = Tmp['mean'].plot(linewidth=0.5, label='mean').get_figure()
    fig = Tmp['max'].plot(linewidth=0.5, marker='.', label='max').get_figure()
    fig.legend()
    fig.savefig("tmp.png", dpi = fig.dpi)



def requete_in_year(station, indicateur, year=2010):
    """ By year
    """
    Spatial = D.filter(lambda d : (d['station'] == station) & (d[indicateur] is not None) & (d['year'] is not None))
    Spatial = Spatial.filter(lambda d : (d['year'] == year))
    # Map indicateurs importants
    Spatial = Spatial.map(lambda d: (d[indicateur], d['year'], d['month'], d['day']))
    Data_Year = map_reduce(Spatial, 1)
    # print(Data_Trimester.collect())
    # return Data_Trimester

    Tmp = np.array(Data_Year.collect())
    Tmp = pd.DataFrame(Tmp).sort_values(by=0)
    # print(Tmp)
    if Tmp.empty:
        print("Empty data. No result")
        return

    Tmp.columns = ['date', 'mean', 'min', 'max', 'std']
    Tmp['month'] = Tmp['date'].apply(lambda x: int(x[0]))
    Tmp['year'] = Tmp['date'].apply(lambda x: int(x[1]))
    # print(Tmp)

    plt.clf()
    plt.title("Need a name")
    plt.errorbar(Tmp['month'], Tmp['mean'], Tmp['std'], fmt='ok', lw=3)
    plt.errorbar(Tmp['month'], Tmp['mean'], [Tmp['mean'] - Tmp['min'], Tmp['max'] - Tmp['mean']], 
                 fmt='.k', ecolor='gray', lw=1)
    plt.savefig('tmp1.png')

    Tmp = Tmp.set_index('month')
    plt.clf()
    plt.title("Need a name")
    fig = Tmp['min'].plot(linewidth=0.5, marker='.', label='min').get_figure()
    fig = Tmp['mean'].plot.bar(color='green', alpha=.5).get_figure()
    # fig = Tmp['mean'].plot(linewidth=0.5, label='mean').get_figure()
    fig = Tmp['max'].plot(linewidth=0.5, marker='o', label='max').get_figure()
    fig.legend()
    fig.savefig("tmp2.png", dpi = fig.dpi)


def requete_lon_lat_interval(lon, lat, indicateur, from_year, to_year):
    _, station_name = distance_to_station(lon, lat)
    requete_interval(station_name, indicateur=indicateur, from_year=from_year, to_year=to_year)
    # plot_interval(Data)

def requete_lon_lat_in_year(lon, lat, indicateur, year):
    _, station_name = distance_to_station(lon, lat)
    requete_in_year(station_name, indicateur=indicateur, year=year)
    # plot_in_year(Data)


if __name__ == "__main__":
    # plot_temperature("EFKI", from_year=None, to_year=None, year = 2005)
    requete_interval("EFKI", indicateur="temperature_fahrenheit", from_year=2007, to_year=2009)
    requete_in_year("EFMA", indicateur="temperature_fahrenheit", year=2010)