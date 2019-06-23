import sys
import re
import logging
from pyspark import SparkContext
import numpy as np
from sklearn.cluster import KMeans
import numpy as np

logging.getLogger().setLevel(logging.INFO)

def extract_data(start, end):
    # Check parameters
    dateparser = re.compile("(?P<year>\d+)-(?P<month>\d+)-(?P<day>\d+)")

    start_parsed = dateparser.match(start)
    end_parsed = dateparser.match(end)

    if not start_parsed or not end_parsed:
        logging.critical('Les dates doivent être au format year-month-day.')
        return

    start_data = start_parsed.groupdict()
    end_data = end_parsed.groupdict()


    # Extract data
    from cassandra.cluster import Cluster
    cluster = Cluster()
    session = cluster.connect('finland_weather_metar')

    query = '''
        SELECT station, latitude, longitude, temperature_fahrenheit
        FROM temporal
        WHERE year >= ''' + str(start_data['year']) + '''
        AND month >= ''' + str(start_data['month']) + '''
        AND day >= ''' + str(start_data['day']) + '''
        AND year <= ''' + str(end_data['year']) + '''
        AND month <= ''' + str(end_data['month']) + '''
        AND day <= ''' + str(end_data['day']) + '''
        ALLOW FILTERING;
    '''

    logging.info('Collecting data...')
    result = session.execute(query)

    for r in result:
        station = r[0]
        latitude = r[1]
        longitude = r[2]
        temperature = r[3]
        if (station and latitude and longitude and temperature):
            yield r

def calc_moy_key(data):
    (key, (n, total)) = data
    return ([key, total/n])

def cluster_by_period(start, end):

    sc = SparkContext.getOrCreate()
    D = sc.parallelize(extract_data(start, end))

    # Compute mean temperature by station
    map = D.map(lambda data: ((data[0], data[1], data[2]), np.array([1, data[3]])))
    sum_temp_by_station = map.reduceByKey(lambda a, b : a + b)

    mean_temp_by_station = sum_temp_by_station.map(calc_moy_key)
    for val in mean_temp_by_station.take(5):
        print(val)

    # KMeans
    X = mean_temp_by_station.map(lambda data: data[1])
    kmeans = KMeans(n_clusters=2, random_state=0).fit(np.array(X.collect()).reshape(-1, 1))

    print(kmeans.labels_)
    print(kmeans.cluster_centers_)
    
    logging.info('Finished')

cluster_by_period(sys.argv[1], sys.argv[2])
