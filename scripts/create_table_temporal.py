

def create_table_temporal (session):
    query = '''
        CREATE TABLE temporal(
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
            PRIMARY KEY ((year, month), day, hour, minute, station)
        );
    '''

    session.execute(query)
