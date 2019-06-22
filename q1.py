import os
import cassandra
import numpy as np
import pandas as pd
import seaborn as sns
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

def get_data(station, indicateur, from_year = None, to_year = None, year = None):
    """ Get data about about a station and its indicateurs concerned
    """
    # Filter by station and indicateur
    Spatial = D.filter(lambda d : (d['station'] == station) & (d[indicateur] is not None) & (d['year'] is not None))

    # Filter by time
    Data_Year = None
    if from_year is not None and to_year is not None:
        Spatial = Spatial.filter(lambda d : (from_year <= d['year'] <= to_year))
    elif year is not None:
        Spatial = Spatial.filter(lambda d : (d['year'] == year))

    # Map indicateurs important
    Spatial = Spatial.map(lambda d: (d[indicateur], d['year'], d['month'], d['day']))

    # MapReduce by year (in case exist 'from_year' and 'to_year')
    Data_Year = Spatial.map(lambda d: ((d[1]), np.array([1, d[0], d[0], d[0]])))
    Data_Year = Data_Year.reduceByKey(lambda a, b : (a[0]+b[0], a[1]+b[1], min(a[2], b[2]), max(a[3], b[3])))

    # MapReduce by trimester (in case exist 'year')
    Data_Trimester = Spatial.map(lambda d: (((d[2]-1) // 3), np.array([1, d[0], d[0], d[0]])))
    Data_Trimester = Data_Trimester.reduceByKey(lambda a, b : (a[0]+b[0], a[1]+b[1], min(a[2], b[2]), max(a[3], b[3])))

    # MapRedude by month (in case exist 'month')
    Data_Month = Spatial.map(lambda d: ((d[2]), np.array([1, d[0], d[0], d[0]])))
    Data_Month = Data_Month.reduceByKey(lambda a, b : (a[0]+b[0], a[1]+b[1], min(a[2], b[2]), max(a[3], b[3])))

    return Data_Year, Data_Month, Data_Trimester

def plot_temperature(station, from_year = None, to_year = None, year = None):
    Temp_Year, Temp_Month, Temp_Trimester = get_data(station, 'relative_humidity', from_year, to_year, year)
    # Dew_Month, Dew_Trimester = get_data(station, 'dew_point_temperature')

    Temp_Month = np.array(Temp_Month.collect())
    Temp_Month = pd.DataFrame(Temp_Month)
    
    Temp_Trimester = np.array(Temp_Trimester.collect())
    Temp_Trimester = pd.DataFrame(Temp_Trimester)

    Temp_Year = np.array(Temp_Year.collect())
    Temp_Year = pd.DataFrame(Temp_Year).sort_values(by=0)

    L = []
    for i in Temp_Month[1]:
        L.append(i[3])

    fig = plt.figure()
    plt.plot(Temp_Month[0], L)
    fig.savefig('temp3.png', dpi=fig.dpi)


# def plot_wind():
#     pass

# def plot_wind_direction():
#     pass

# https://www.shanelynn.ie/analysis-of-weather-data-using-pandas-python-and-seaborn/
if __name__ == "__main__":
    plot_temperature("EFKI", from_year=None, to_year=None, year = 2005)