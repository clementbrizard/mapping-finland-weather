#%%
# Connect to keyspace
from cassandra.cluster import Cluster
cluster = Cluster()
session = cluster.connect('finland_weather_metar')

#%%
# Create table template
query = '''
    CREATE TABLE spatial(
        station text,
        year int,
        month int,
        day int,
        hour int,
        minute int,
        longitude float,
        latitude float,
        temperature_fahrenheit float,
        dew_point_temperature float,
        relative_humidity float, -- en %
        wind_direction float,
        wind_speed float, -- knots
        one_hour_precipitation float,
        pressure_altimeter float, -- inches
        sea_level_pressure float, -- millibar
        visibility float, -- miles
        wind_gust float, -- knots
        sky_cover_level_1 text,
        sky_cover_level_2 text,
        sky_cover_level_3 text,
        sky_cover_level_4 text,
        sky_altitude_level_1 float, --feet
        sky_altitude_level_2 float,
        sky_altitude_level_3 float,
        sky_altitude_level_4 float,
        weather_codes text,
        ice_accreation_1h float,
        ice_accreation_3h float,
        ice_accreation_6h float,
        peak_wind_gust float, -- mph
        peak_wind_direction float, -- dec
        peak_wind_time float,
        feel float,
        metar text,
        PRIMARY KEY ((station, longitude, latitude), year, month, day)
    );
'''
session.execute(query)

#%%
import re
import csv

#%%
def loadata(filename):
    dateparser = re.compile("(?P<year>\d+)-(?P<month>\d+)-(?P<day>\d+) (?P<hour>\d+):(?P<minute>\d+)")
    with open(filename) as f:
        for r in csv.DictReader(f):
            date_parsed = dateparser.match(r["valid"])
            if not date_parsed:
                continue
            date = date_parsed.groupdict()
            data = {}
            data["station"] = r["station"] if r["station"] != "null" else None
            data["year"] = int(date["year"])
            data["month"] = int(date["month"])
            data["day"] = int(date["day"])
            data["hour"] = int(date["hour"])
            data["minute"] = int(date["minute"])
            data["longitude"] = float(r["lon"]) if r["lon"] != "null" else None
            data["latitude"] = float(r["lat"]) if r["lat"] != "null" else None
            data["temperature_fahrenheit"] = float(r["tmpf"]) if r["tmpf"] != "null" else None
            data["dew_point_temperature"] = float(r["dwpf"]) if r["dwpf"] != "null" else None
            data["relative_humidity"] = float(r["relh"]) if r["relh"] != "null" else None
            data["wind_direction"] = float(r["drct"]) if r["drct"] != "null" else None
            data["wind_speed"] = float(r["sknt"]) if r["sknt"] != "null" else None
            data["one_hour_precipitation"] = float(r["p01i"]) if r["p01i"] != "null" else None
            data["pressure_altimeter"] = float(r["alti"]) if r["alti"] != "null" else None
            data["sea_level_pressure"] = float(r["mslp"]) if r["mslp"] != "null" else None
            data["visibility"] = float(r["vsby"]) if r["vsby"] != "null" else None
            data["wind_gust"] = float(r["gust"]) if r["gust"] != "null" else None
            data["sky_cover_level_1"] = r["skyc1"] if r["skyc1"] != "null" else None
            data["sky_cover_level_2"] = r["skyc2"] if r["skyc2"] != "null" else None
            data["sky_cover_level_3"] = r["skyc3"] if r["skyc3"] != "null" else None
            data["sky_cover_level_4"] = r["skyc4"] if r["skyc4"] != "null" else None
            data["sky_altitude_level_1"] = float(r["skyl1"]) if r["skyl1"] != "null" else None
            data["sky_altitude_level_2"] = float(r["skyl2"]) if r["skyl2"] != "null" else None
            data["sky_altitude_level_3"] = float(r["skyl3"]) if r["skyl3"] != "null" else None
            data["sky_altitude_level_4"] = float(r["skyl4"]) if r["skyl4"] != "null" else None
            data["weather_codes"] = r["wxcodes"] if r["wxcodes"] != "null" else None
            data["ice_accreation_1h"] = r["ice_accretion_1hr"] if r["ice_accretion_1hr"] != "null" else None
            data["ice_accreation_3h"] = r["ice_accretion_3hr"] if r["ice_accretion_3hr"] != "null" else None
            data["ice_accreation_6h"] = r["ice_accretion_6hr"] if r["ice_accretion_6hr"] != "null" else None
            data["peak_wind_gust"] = int(r["peak_wind_gust"]) if r["peak_wind_gust"] != "null" else None
            data["peak_wind_direction"] = int(r["peak_wind_drct"]) if r["peak_wind_drct"] != "null" else None
            data["peak_wind_time"] = r["peak_wind_time"] if r["peak_wind_time"] != "null" else None
            data["feel"] = float(r["feel"]) if r["feel"] != "null" else None
            data["metar"] = r["metar"] if r["metar"] != "null" else None
            yield data

#%%
def writecassandra(csvfilename, session):
    data = loadata(csvfilename)
    query = """
        INSERT INTO spatial(
            station,
            year,
            month,
            day,
            hour,
            minute,
            longitude,
            latitude,
            temperature_fahrenheit,
            dew_point_temperature,
            relative_humidity,
            wind_direction,
            wind_speed,
            one_hour_precipitation,
            pressure_altimeter,
            sea_level_pressure,
            visibility,
            wind_gust,
            sky_cover_level_1,
            sky_cover_level_2,
            sky_cover_level_3,
            sky_cover_level_4,
            sky_altitude_level_1,
            sky_altitude_level_2,
            sky_altitude_level_3,
            sky_altitude_level_4,
            weather_codes,
            ice_accreation_1h,
            ice_accreation_3h,
            ice_accreation_6h,
            peak_wind_gust,
            peak_wind_direction,
            peak_wind_time,
            feel,
            metar
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    tmp = 0
    for r in data:
        tmp += 1
        print(tmp)
        t = (
            r["station"],
            r["year"],
            r["month"],
            r["day"],
            r["hour"],
            r["minute"],
            r["longitude"],
            r["latitude"],
            r["temperature_fahrenheit"],
            r["dew_point_temperature"],
            r["relative_humidity"],
            r["wind_direction"],
            r["wind_speed"],
            r["one_hour_precipitation"],
            r["pressure_altimeter"],
            r["sea_level_pressure"],
            r["visibility"],
            r["wind_gust"],
            r["sky_cover_level_1"],
            r["sky_cover_level_2"],
            r["sky_cover_level_3"],
            r["sky_cover_level_4"],
            r["sky_altitude_level_1"],
            r["sky_altitude_level_2"],
            r["sky_altitude_level_3"],
            r["sky_altitude_level_4"],
            r["weather_codes"],
            r["ice_accreation_1h"],
            r["ice_accreation_3h"],
            r["ice_accreation_6h"],
            r["peak_wind_gust"],
            r["peak_wind_direction"],
            r["peak_wind_time"],
            r["feel"],
            r["metar"]
        )
        session.execute(query, t)

writecassandra("asos.txt", session)

#%%
