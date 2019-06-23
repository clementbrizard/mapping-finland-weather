# Projet NF26

## Exercice 1 :

Executer sur `ipython` (pas sur `pyspark`)
> $ ipython
> %run stats_of_station.py

List of some indicateurs useful :
```
temperature_fahrenheit, dew_point_temperature, relative_humidity, wind_speed, one_hour_precipitation, ...
```

List of stations are in 'station_list.txt'. Some example :
```
EFHK, EFMA, EFKA, EFSA, ...
```

### Some examples of function :

---- Stats for a station in range of year
> requete_interval("EFKI", indicateur="dew_point_temperature", from_year=2006, to_year=2012)

---- Stats for a station in a year specific
> requete_in_year("EFMA", indicateur="temperature_fahrenheit", year=2010)

---- Stats for a (lon, lat) in range of year
> requete_lon_lat_interval(28.5, 61.5, "dew_point_temperature", from_year=2006, to_year=2012)

---- Stats for a (lon, lat) in a year specific
> requete_lon_lat_in_year(28.5, 61.5, "temperature_fahrenheit", year=2011)

**Graphics created will be all in folder results/stats**
