import re
import csv

def load_data(filename):
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
