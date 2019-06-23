import sys
import re
import logging
from cassandra.cluster import Cluster
from pyspark import SparkContext
import numpy as np
from sklearn.cluster import KMeans
import numpy as np
import matplotlib.pyplot as plt
from kneed import KneeLocator
import os
from mpl_toolkits.basemap import Basemap
from matplotlib.colors import Colormap

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

    # KMeans
    # Fist we determine the optimal k with elbow method
    logging.info('Clustering...')
    X = mean_temp_by_station.map(lambda data: data[1])
    X = np.array(X.collect()).reshape(-1, 1)
    sum_of_squared_distances = []
    K = range(1,10)
    for k in K:
        km = KMeans(n_clusters=k)
        km = km.fit(X)
        sum_of_squared_distances.append(km.inertia_)
    
    x = list(range(1,10))
    y = sum_of_squared_distances
    kn = KneeLocator(x, y, S=1.0, curve='convex', direction='decreasing', interp_method='polynomial')
    plt.xlabel('k')
    plt.ylabel('sum_of_squared_distances')
    plt.title('Elbow method for optimal k between {} and {}'.format(start, end))
    plt.xticks(range(1,9))
    plt.plot(x, y, 'bx-')
    plt.vlines(kn.knee, plt.ylim()[0], plt.ylim()[1], linestyles='dashed')

    if os.path.isfile('elbow.png'):
        os.remove('elbow.png')
    plt.savefig('elbow.png')
    plt.clf()

    # Then we cluster with the computed optimal k
    km = KMeans(n_clusters=kn.knee)
    km = km.fit(X)

    # Build map
    logging.info('Building map...')

    lons=[]
    lats=[]
    vals=km.labels_

    for val in mean_temp_by_station.collect():
        lats.append(val[0][1])
        lons.append(val[0][2])

    map = Basemap(projection='merc', llcrnrlon=19.08,llcrnrlat=59.45,urcrnrlon=31.59,urcrnrlat=70.09, resolution='i') 

    map.drawmapboundary(fill_color='aqua')
    map.fillcontinents(color='#cc9955',lake_color='aqua', zorder=1)
    map.drawcoastlines()
    map.drawcountries()

    x, y = map(lons, lats)

    map.scatter(x, y, c = vals, cmap = plt.cm.get_cmap('Set1', kn.knee), zorder = 2)

    plt.title('Clustering des stations (sur la température) entre {} et {}'.format(start, end))

    if os.path.isfile('clusters_map.png'):
        os.remove('clusters_map.png')
    plt.savefig('clusters_map.png')

    logging.info('Success : update your files and check \'clusters_map.png\' !')
    logging.info('NB : you can also check \'elbow.png\' to see how we chose the clusters number.')
    plt.clf()

cluster_by_period(sys.argv[1], sys.argv[2])
