import os
import csv
import cassandra
import numpy as np
import pandas as pd
import seaborn as sns
import mpu # to calculate distance using longitude and latitude
from statsmodels.tsa.seasonal import seasonal_decompose
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
        Return Spark DataFrame
    """
    table_df = sqlContext.read.format("org.apache.spark.sql.cassandra") \
                         .options(keyspace=keyspace_name, table=table_name).load()
    return table_df


D = get_table_df("finland_weather_metar", "spatial").rdd


def distance_to_station(longitude, latitude):
    """ Find the nearest station by using longitude and latitude 
        Return (dist_to_station, 'name_station')
    """
    # Load list of stations from file
    with open("station_list.txt") as f:
        list_station = []
        for r in csv.DictReader(f):
            list_station.append((r['station'],float(r['longitude']),float(r['latitude'])))

    # Find station nearest distance
    min_station = (99999999, "")
    for station in list_station:
        dist = mpu.haversine_distance((latitude, longitude), (station[2], station[1]))
        if dist < min_station[0]: min_station = (dist, station[0])

    return min_station


def map_reduce(Data, type):
    """ Do MapReduce on Spark RDD
        Return (key, mean, min, max, std)
    """
    # Map follow type
    if type == 0 or type == 2: # Year Month
        Tmp = Data.map(lambda d: ((d[1], d[2]), np.array([1, d[0], d[0], d[0], d[0]**2])))
    elif type == 1: # Trimester
        Tmp = Data.map(lambda d: (((d[2]-1) // 3 + 1, d[1]), np.array([1, d[0], d[0], d[0], d[0]**2])))
        # Tmp = Data.map(lambda d: ((d[2], d[3]), np.array([1, d[0], d[0], d[0], d[0]**2])))

    Tmp = Tmp.reduceByKey(lambda a, b : (a[0]+b[0], a[1]+b[1], min(a[2], b[2]), max(a[3], b[3]), a[4]+b[4]))
    Tmp = Tmp.map(lambda d: ((d[0]), d[1][1]/d[1][0], d[1][2], d[1][3], np.sqrt(-(d[1][1]/d[1][0])**2 + d[1][4]/d[1][0])))
    return Tmp


def requete_interval(station, indicateur, from_year=2005, to_year=2014):
    """ Make plot by interval of years
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

    # Check if data is empty
    if Tmp.empty:
        print("Empty data. No result")
        return

    # Transform columns
    Tmp.columns = ['date', 'mean', 'min', 'max', 'std']
    Tmp['month'] = Tmp['date'].apply(lambda x: int(x[1]))
    Tmp['year'] = Tmp['date'].apply(lambda x: int(x[0]))
    Tmp['date'] = Tmp['date'].apply(lambda x: pd.to_datetime(str(x[0]) + '-' + str(x[1])))

    # Heatmap
    plt.close()
    heat_map_tab = Tmp.pivot_table(index="month", columns="year",values="mean", aggfunc="sum").fillna(0)
    heat_map = sns.heatmap(heat_map_tab) # annot=True
    heat_map.set(title="Mean of {} in {} ({}-{})".format(indicateur, station, from_year, to_year))
    heat_map.get_figure().savefig('results/stats/{}-heatmap-{}-{}-{}.png'.format(station, indicateur, from_year, to_year))

    Tmp = Tmp.set_index('date')
    # print(Tmp)

    # Lineplot
    plt.close()
    plt.title("{} in {} ({}-{})".format(indicateur, station, from_year, to_year))
    line = Tmp['min'].plot(linewidth=0.5, marker='.', label='min').get_figure()
    line = Tmp['mean'].plot(linewidth=0.5, label='mean').get_figure()
    line = Tmp['max'].plot(linewidth=0.5, marker='.', label='max').get_figure()
    line.legend()
    line.savefig("results/stats/{}-lineplot-{}-{}-{}.png".format(station, indicateur, from_year, to_year))

    # Seasonality
    plt.close()
    season = Tmp.pivot_table(index="date", values="mean", aggfunc="sum").fillna(0)
    season = seasonal_decompose(season, model='additive', freq=4)
    season.plot().savefig("results/stats/{}-seasonality-{}-{}-{}.png".format(station, indicateur, from_year, to_year))


def requete_in_year(station, indicateur, year=2010):
    """ Make plot by specific year
    """
    Spatial = D.filter(lambda d : (d['station'] == station) & (d[indicateur] is not None) & (d['year'] is not None))
    Spatial = Spatial.filter(lambda d : (d['year'] == year))
    # Map indicateurs importants
    Spatial = Spatial.map(lambda d: (d[indicateur], d['year'], d['month'], d['day']))

    Data_Trimester = map_reduce(Spatial, 1)
    Data_Trimester = np.array(Data_Trimester.collect())
    Data_Trimester = pd.DataFrame(Data_Trimester).sort_values(by=0)

    if Data_Trimester.empty:
        print("Empty data in trimester. No result")
        return

    Data_Trimester.columns = ['date', 'mean', 'min', 'max', 'std']
    Data_Trimester['trimester'] = Data_Trimester['date'].apply(lambda x: int(x[0]))
    Data_Trimester['year'] = Data_Trimester['date'].apply(lambda x: int(x[1]))


    Data_Month = map_reduce(Spatial, 2)
    Data_Month = np.array(Data_Month.collect())
    Data_Month = pd.DataFrame(Data_Month).sort_values(by=0)
    if Data_Month.empty:
        print("Empty data in month. No result")
        return

    Data_Month.columns = ['date', 'mean', 'min', 'max', 'std']
    Data_Month['month'] = Data_Month['date'].apply(lambda x: int(x[1]))
    Data_Month['year'] = Data_Month['date'].apply(lambda x: int(x[0]))


    plt.close()
    plt.subplot(2, 1, 1)
    plt.title("{} in {} ({}).png".format(indicateur, station, year))
    Data_Month = Data_Month.set_index('month')
    line = Data_Month['min'].plot(linewidth=0.5, marker='.', label='min').get_figure()
    line = Data_Month['mean'].plot(linewidth=0.5, marker='', label='mean').get_figure()
    line = Data_Month['max'].plot(linewidth=0.5, marker='.', label='max').get_figure()

    plt.subplot(2, 1, 2)
    plt.xlabel("trimester")
    plt.errorbar(Data_Trimester['trimester'], Data_Trimester['mean'], Data_Trimester['std'], fmt='ok', lw=3)
    plt.errorbar(Data_Trimester['trimester'], Data_Trimester['mean'], [Data_Trimester['mean'] - Data_Trimester['min'], Data_Trimester['max'] - Data_Trimester['mean']], 
                 fmt='.k', ecolor='gray', lw=1)
    plt.tight_layout()
    plt.savefig("results/stats/{}-{}-{}.png".format(station, indicateur, year))


def requete_lon_lat_interval(lon, lat, indicateur, from_year, to_year):
    _, station_name = distance_to_station(lon, lat)
    requete_interval(station_name, indicateur=indicateur, from_year=from_year, to_year=to_year)


def requete_lon_lat_in_year(lon, lat, indicateur, year):
    _, station_name = distance_to_station(lon, lat)
    requete_in_year(station_name, indicateur=indicateur, year=year)


if __name__ == "__main__":
    print("""
List of some indicateurs useful :
temperature_fahrenheit, dew_point_temperature, relative_humidity, wind_speed, one_hour_precipitation, ...
List of stations are in 'station_list.txt'
|
Some examples of function :
|
---- Stats for a station in range of year
requete_interval("EFKI", indicateur="dew_point_temperature", from_year=2006, to_year=2012)
|
---- Stats for a station in a year specific
requete_in_year("EFMA", indicateur="temperature_fahrenheit", year=2010)
|
---- Stats for a (lon, lat) in range of year
requete_lon_lat_interval(28.5, 61.5, "dew_point_temperature", from_year=2006, to_year=2012)
|
---- Stats for a (lon, lat) in a year specific
requete_lon_lat_in_year(28.5, 61.5, "temperature_fahrenheit", year=2011)
|
*** Graphics created will be all in folder results/stats
    """)