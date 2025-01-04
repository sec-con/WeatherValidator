import openmeteo_requests
import sqlite3
import requests_cache
import pandas as pd
import datetime
from retry_requests import retry
import numpy as np

#SQLITE CONFIG
connection = sqlite3.connect("../data/weather.db")
cursor = connection.cursor()
cursor.execute("CREATE TABLE IF NOT EXISTS locale (coordinates TEXT, elevation TEXT, timezone TEXT, timezone_offset TEXT)")
cursor.execute("DELETE FROM locale")

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after = 1800)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

# Make sure all required weather variables are listed here
# The order of variables in hourly or daily is important to assign them correctly below
url = "https://api.open-meteo.com/v1/forecast"
params = {
	"latitude": 51.561733,
	"longitude": 0.645287,
	"current": "temperature_2m",
	"hourly": ["temperature_2m", "rain", "showers", "snowfall", "weather_code", "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m","is_day"],
	"wind_speed_unit": "mph",
	"timezone": "Europe/London",
	"past_days": 1,
	"forecast_days": 3,
	"models": "ukmo_seamless"
}
responses = openmeteo.weather_api(url, params=params)

response = responses[0]
cursor.execute("INSERT INTO locale (coordinates, elevation, timezone, timezone_offset) VALUES (?, ?, ?, ?)",
               (f"{response.Latitude()}°N {response.Longitude()}°E", 
                response.Elevation(), 
                response.Timezone(), 
                response.UtcOffsetSeconds()))
connection.commit()

current = response.Current()
hourly = response.Hourly()

hourly_temperature_2m = np.round(hourly.Variables(0).ValuesAsNumpy(), decimals=0)
hourly_rain = np.round(hourly.Variables(1).ValuesAsNumpy(), decimals=0)
hourly_showers = np.round(hourly.Variables(2).ValuesAsNumpy(), decimals=2)
hourly_snowfall = np.round(hourly.Variables(3).ValuesAsNumpy(), decimals=2)
hourly_weather_code = hourly.Variables(4).ValuesAsNumpy()
hourly_wind_speed_10m = np.round(hourly.Variables(5).ValuesAsNumpy(), decimals=0)
hourly_wind_direction_10m = np.round(hourly.Variables(6).ValuesAsNumpy(), decimals=0)
hourly_wind_gusts_10m = np.round(hourly.Variables(7).ValuesAsNumpy(), decimals=0)
hourly_is_day_night = hourly.Variables(8).ValuesAsNumpy()

##import pandas as pd

hourly_data = {
    "forecast_date_time": pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left"
    ).tz_localize(None)  # Remove the timezone information
}

##print(hourly_data)


date_time = datetime.datetime.fromtimestamp(current.Time())
date_time_minus_one_hour = date_time - datetime.timedelta(hours=1)
formatted_date_time = date_time_minus_one_hour.strftime('%Y-%m-%d %H:%M:%S')


hourly_data["forecast_day_night"] = hourly_is_day_night
hourly_data["snapshot_date_time"] = formatted_date_time
hourly_data["temperature"] = hourly_temperature_2m
hourly_data["rainfall"] = hourly_showers + hourly_rain
hourly_data["snowfall"] = hourly_snowfall
hourly_data["weather_code"] = hourly_weather_code
hourly_data["wind_speed"] = hourly_wind_speed_10m
hourly_data["wind_direction"] = hourly_wind_direction_10m
hourly_data["wind_gusts"] = hourly_wind_gusts_10m
hourly_data["observed"] = hourly_data["snapshot_date_time"] > hourly_data["forecast_date_time"]
hourly_dataframe = pd.DataFrame(data = hourly_data)
hourly_dataframe.to_sql('forecast_data', connection, if_exists='append', index=False)

connection.commit()



# SQL command to remove duplicates
sql_remove_duplicates = '''
DELETE FROM forecast_data
WHERE rowid NOT IN (
    SELECT MIN(rowid)
    FROM forecast_data
    GROUP BY forecast_date_time, forecast_day_night, snapshot_date_time, temperature, rainfall, snowfall, weather_code, wind_speed, wind_direction, wind_gusts
);
'''
cursor.execute(sql_remove_duplicates)
connection.commit()


sql_remove_duplicate_observed = '''
DELETE
FROM forecast_data
WHERE observed = 1
AND rowid NOT IN (
    SELECT rowid
    FROM (
        SELECT 
            forecast_date_time, 
            MAX(snapshot_date_time) AS latest_time, 
            MAX(rowid) AS rowid
        FROM 
            forecast_data
        WHERE 
            observed = 1
        GROUP BY 
            forecast_date_time
    ) AS latest_records
);
'''



cursor.execute(sql_remove_duplicate_observed)
connection.commit()

connection.close()

print(hourly_dataframe)
