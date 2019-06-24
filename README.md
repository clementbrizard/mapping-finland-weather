# NF26 - METAR project

## Exercise 1 : stats for a given place

Launch in `ipython` (not `pyspark`) :
```console
$ cd nf26-metar
$ ipython
In [1]: %run stats_of_station.py
```

List of useful indicators :
```
temperature_fahrenheit, dew_point_temperature, relative_humidity, wind_speed, one_hour_precipitation, ...
```

List of stations in `station_list.txt`. Examples :
```
EFHK, EFMA, EFKA, EFSA, ...
```

### Examples of available functions :

---- Stats for a station in range of years
```console
$ from stats_of_station import *
$ requete_interval("EFKI", indicateur="dew_point_temperature", from_year=2006, to_year=2012)
```

---- Stats for a station in a specific year 
```console
$ from stats_of_station import *
$ requete_in_year("EFMA", indicateur="temperature_fahrenheit", year=2010)
```

---- Stats for a (lon, lat) in range of years
```console
$ from stats_of_station import *
$ requete_lon_lat_interval(28.5, 61.5, "dew_point_temperature", from_year=2006, to_year=2012)
```

---- Stats for a (lon, lat) in a specific year 
```console
$ from stats_of_station import *
$ requete_lon_lat_in_year(28.5, 61.5, "temperature_fahrenheit", year=2011)
```

**Graphs created will be all in folder `results/stats`**

## Exercise 2 : map for a given instant

Launch in `ipython` :
```console
$ ipython
In [1]: %run map_by_indicator_and_time indicator yyyy-mm-dd hh
```

An instant is considered as a date and an hour. **Find the map in `results/map_by_indicator`**. See previous section for a list of interesting indicators.

## Exercise 3 : stations clustering for a given period of time

Launch in `pyspark` :
```console
$ source /pyspark.env 
$ pyspark
In [1]: %run cluster_by_period yyyy yyyy
```
A period is considered as a range of years. The first argument is the start year, the second one is the end year. You should better specify an end year greater than 2009 as we only have data for one station until 2010. **Find the map in `results/cluster_by_period`**.

