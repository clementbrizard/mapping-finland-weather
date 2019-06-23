import os
from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt
from matplotlib.colors import Colormap
import numpy as np
import sys
import re
import logging

logging.getLogger().setLevel(logging.INFO)

def map_by_indicator_and_time(indicator, date, hour): 

    # Check parameters
    dateparser = re.compile("(?P<year>\d+)-(?P<month>\d+)-(?P<day>\d+)")

    date_parsed = dateparser.match(date)

    if not date_parsed:
        logging.critical('La date doit être au format year-month-day.')
        return

    date_data = date_parsed.groupdict()


    # Extract data
    from cassandra.cluster import Cluster
    cluster = Cluster()
    session = cluster.connect('finland_weather_metar')

    query = '''
        SELECT station, latitude, longitude, ''' + indicator + '''
        FROM temporal
        WHERE year = ''' + str(date_data['year']) + '''
        AND month = ''' + str(date_data['month']) + '''
        AND day = ''' + str(date_data['day']) + '''
        AND hour = ''' + str(hour) + ''';
    '''

    logging.info('Collecting data...')
    result = session.execute(query)

    lons = []
    lats = []
    vals = []

    # Group measures by station
    dict_by_station = {}
    for r in result:
        station = r[0]
        lat = r[1]
        lon = r[2]
        val = r[3]
        if (station and lat and lon and val):
            if (not station in dict_by_station):
                dict_by_station[station] = val
                lons.append(lon)
                lats.append(lat)
            
            else:
                dict_by_station[station] = (dict_by_station[station] + val) / 2

    logging.info('{} stations had data for this date :'.format(len(dict_by_station)))
    for key, value in dict_by_station.items():
        vals.append(value)
        print('{} : {}'.format(key, round(value, 1)))


    # Build map
    logging.info('Building map...')
    map = Basemap(projection='merc', llcrnrlon=19.08,llcrnrlat=59.45,urcrnrlon=31.59,urcrnrlat=70.09, resolution='i') 

    map.drawmapboundary(fill_color='aqua')
    map.fillcontinents(color='#cc9955',lake_color='aqua', zorder=1)
    map.drawcoastlines()
    map.drawcountries()

    x, y = map(lons, lats)

    map.scatter(x, y, c = vals, vmin = min(vals), vmax = max(vals), cmap = 'hot_r', zorder = 2)
    cbar = map.colorbar()
    cbar.set_label(indicator)

    plt.title('{} in Finland the {} at {}H'.format(indicator, date, hour))

    filename = 'results/map_by_indicator/{}_{}_{}'.format(indicator, date, hour)
    if os.path.isfile(filename):
        os.remove(filename)
    plt.savefig(filename)

    logging.info('Success : update your files and check the result in \'results/map_by_indicator\' !')
    plt.clf()

map_by_indicator_and_time(sys.argv[1], sys.argv[2], sys.argv[3])