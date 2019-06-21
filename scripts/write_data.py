
def write_cassandra(session, data):
    query = """
        INSERT INTO temporal(
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
