import os
import numpy as np
import cassandra
from pyspark import SparkContext
from pyspark.sql import SQLContext

os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.11:2.3.0 \
                                     --conf spark.cassandra.connection.host=localhost pyspark-shell'

sc = SparkContext()
sqlContext = SQLContext(sc)

def get_table_df(keyspace_name, table_name):
    table_df = sqlContext.read.format("org.apache.spark.sql.cassandra").options(keyspace=keyspace_name, table=table_name).load()
    return table_df

D = get_table_df("finland_weather_metar", "spatial").rdd

D2 = D.map(lambda d: (d[0], d[1], d[2]))

D3 = D2.reduceByKey(lambda a,b: a+b)


def get_hist(station, indicateur):
    Spatial = D.filter(lambda d : d['station'] == station).map(lambda d: ((d['year'], d['month'], d['day'], d['hour'], d['minute'], d[indicateur])))
    Season = Spatial.map()