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
    # Extract data
    cluster = Cluster()
    session = cluster.connect('finland_weather_metar')

    query = '''
        SELECT station, latitude, longitude, temperature_fahrenheit, dew_point_temperature, feel
        FROM temporal
        WHERE year >= ''' + str(start) + '''
        AND year <= ''' + str(end) + '''
        ALLOW FILTERING;
    '''

    logging.info('Collecting data...')
    result = session.execute(query)

    for r in result:
        station = r[0]
        latitude = r[1]
        longitude = r[2]
        temperature = r[3]
        dew_point = r[4]
        feel = r[5]
        if (station and latitude and longitude and temperature and dew_point and feel):
            data = {'station': station, 'latitude': latitude, 'longitude': longitude, 'temperature': temperature, 'dew_point': dew_point, 'feel': feel}
            yield data

def calc_moy_key(data):
    (key, (n, total_temp, total_dew_point, total_feel)) = data
    return ([key, round(total_temp/n, 1), round(total_dew_point/n, 1), round(total_feel/n, 1)])

def cluster_by_period(start, end):

    sc = SparkContext.getOrCreate()
    D = sc.parallelize(extract_data(start, end))

    # Compute mean temperature by station
    map = D.map(lambda data: ((data['station'], data['latitude'], data['longitude']), np.array([1, data['temperature'], data['dew_point'], data['feel']])))
    sum_by_station = map.reduceByKey(lambda a, b : a + b)

    mean_by_station = sum_by_station.map(calc_moy_key)
    if (len(mean_by_station.collect()) < 2):
        logging.warning('Only data for one station during this period, so there is one cluster of one station !')
        return

    # KMeans
    # Fist we determine the optimal k with elbow method
    logging.info('Clustering...')
    X = mean_by_station.map(lambda data: [data[1], data[2]])
    X = np.array(X.collect())
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

    filename = 'results/cluster_by_period/{}_{}_elbow.png'.format(start, end)
    if os.path.isfile(filename):
        os.remove(filename)
    plt.savefig(filename)
    plt.clf()

    # Then we cluster with the computed optimal k
    km = KMeans(n_clusters=kn.knee)
    km = km.fit(X)

    # Build map
    logging.info('Building map...')

    lons=[]
    lats=[]
    vals=km.labels_

    for val in mean_by_station.collect():
        lats.append(val[0][1])
        lons.append(val[0][2])

    map = Basemap(projection='merc', llcrnrlon=19.08,llcrnrlat=59.45,urcrnrlon=31.59,urcrnrlat=70.09, resolution='i') 

    map.drawmapboundary(fill_color='aqua')
    map.fillcontinents(color='#cc9955',lake_color='aqua', zorder=1)
    map.drawcoastlines()
    map.drawcountries()

    x, y = map(lons, lats)

    map.scatter(x, y, c = vals, cmap = plt.cm.get_cmap('gist_rainbow', kn.knee), zorder = 2)

    plt.title('Clustering des stations entre {} et {}'.format(start, end), fontsize = 10)

    filename = 'results/cluster_by_period/{}_{}.png'.format(start, end)
    if os.path.isfile(filename):
        os.remove(filename)
    plt.savefig(filename)

    logging.info('Success : update your files and check the result in \'results/cluster_by_period\' !')
    logging.info('NB : you can also check \'elbow.png\' to see how we chose the clusters number.')
    plt.clf()

cluster_by_period(sys.argv[1], sys.argv[2])
