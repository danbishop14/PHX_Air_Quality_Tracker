import json
import snowflake.connector
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()  # take environment variables from .env.

API_KEY = os.getenv('API_KEY')

CITY = 'Phoenix,US'
ZIP = '85254,US'  # Example zip code

def get_coordinates(city=None, zip_code=None):
    if city:
        response = requests.get(f'http://api.openweathermap.org/geo/1.0/direct?q={city}&limit=1&appid={API_KEY}')
        location_data = response.json()
        return location_data[0]['lat'], location_data[0]['lon']
    elif zip_code:
        response = requests.get(f'http://api.openweathermap.org/geo/1.0/zip?zip={zip_code}&appid={API_KEY}')
        location_data = response.json()
        return location_data['lat'], location_data['lon']
    else:
        raise ValueError("Either a city or a zip code must be provided.")

LAT, LON = get_coordinates(city=CITY)  # Or get_coordinates(zip_code=ZIP)

response = requests.get(f'http://api.openweathermap.org/data/2.5/weather?lat={LAT}&lon={LON}&appid={API_KEY}')
response2 = requests.get(f'http://api.openweathermap.org/data/2.5/air_pollution/forecast?lat={LAT}&lon={LON}&appid={API_KEY}')
data = response.json()
data2 = response2.json()


with open(r'C:\Users\Dan\Desktop\Coding\phx_pollen_tracker\data\weather_data.json', 'w') as f:
    json.dump(data, f)
with open(r'C:\Users\Dan\Desktop\Coding\phx_pollen_tracker\data\pollen_data.json', 'w') as f:
    json.dump(data2, f)

conn = snowflake.connector.connect(
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    database=os.getenv('SNOWFLAKE_DATABASE'),
    schema=os.getenv('SNOWFLAKE_SCHEMA')
)
# Create cursor
cur = conn.cursor()

# Create table if not exists
cur.execute("""
    CREATE TABLE IF NOT EXISTS WEATHER_DATA (
        LON FLOAT,
        LAT FLOAT,
        WEATHER_ID NUMBER,
        MAIN VARCHAR,
        DESCRIPTION VARCHAR,
        ICON VARCHAR,
        TEMP FLOAT,
        FEELS_LIKE FLOAT,
        TEMP_MIN FLOAT,
        TEMP_MAX FLOAT,
        PRESSURE NUMBER,
        HUMIDITY NUMBER,
        WIND_SPEED FLOAT,
        WIND_DEG NUMBER,
        WIND_GUST FLOAT,
        CLOUDS_ALL NUMBER,
        COUNTRY VARCHAR,
        SUNRISE TIMESTAMP_NTZ,
        SUNSET TIMESTAMP_NTZ,
        CITY_NAME VARCHAR,
        RECORD_TIMESTAMP TIMESTAMP_NTZ
    )
""")

# Create table if not exists
cur.execute("""
    CREATE TABLE IF NOT EXISTS AIR_QUALITY_DATA (
        LON FLOAT,
        LAT FLOAT,
        DATE TIMESTAMP_NTZ,
        AQI NUMBER,
        CO FLOAT,
        NO FLOAT,
        NO2 FLOAT,
        O3 FLOAT,
        SO2 FLOAT,
        PM2_5 FLOAT,
        PM10 FLOAT,
        NH3 FLOAT,
        RECORD_TIMESTAMP TIMESTAMP_NTZ
    )
""")

# Extract individual values from the data dictionary
weather_data = [
    data['coord']['lon'],
    data['coord']['lat'],
    data['weather'][0]['id'],
    data['weather'][0]['main'],
    data['weather'][0]['description'],
    data['weather'][0]['icon'],
    data['main']['temp'],
    data['main']['feels_like'],
    data['main']['temp_min'],
    data['main']['temp_max'],
    data['main']['pressure'],
    data['main']['humidity'],
    data['wind']['speed'],
    data['wind']['deg'],
    data['wind'].get('gust', None),  # use get method to handle optional field
    data['clouds']['all'],
    data['sys']['country'],
    pd.to_datetime(data['sys']['sunrise'], unit='s').strftime('%Y-%m-%d %H:%M:%S'),  # convert UNIX timestamp to datetime and then to string
    pd.to_datetime(data['sys']['sunset'], unit='s').strftime('%Y-%m-%d %H:%M:%S'),  # convert UNIX timestamp to datetime and then to string
    data['name']
]

# Add timestamp to the weather_data
timestamp = datetime.now()
weather_data.append(timestamp)

# Modify your INSERT statement to include the RECORD_TIMESTAMP
cur.execute("""
    INSERT INTO WEATHER_DATA (
        LON, LAT, WEATHER_ID, MAIN, DESCRIPTION, ICON, TEMP, FEELS_LIKE, TEMP_MIN, TEMP_MAX, PRESSURE, HUMIDITY,
        WIND_SPEED, WIND_DEG, WIND_GUST, CLOUDS_ALL, COUNTRY, SUNRISE, SUNSET, CITY_NAME, RECORD_TIMESTAMP
    )
    VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
""", tuple(weather_data))

# Iterate through the list of forecasts in the air quality data
for forecast in data2['list']:
    # Extract individual values from the forecast dictionary
    air_quality_data = [
        data2['coord']['lon'],
        data2['coord']['lat'],
        pd.to_datetime(forecast['dt'], unit='s').strftime('%Y-%m-%d %H:%M:%S'),  # convert UNIX timestamp to datetime and then to string
        forecast['main']['aqi'],
        forecast['components']['co'],
        forecast['components']['no'],
        forecast['components']['no2'],
        forecast['components']['o3'],
        forecast['components']['so2'],
        forecast['components']['pm2_5'],
        forecast['components']['pm10'],
        forecast['components']['nh3']
    ]

    # Add timestamp to the air_quality_data
    timestamp = datetime.now()
    air_quality_data.append(timestamp)

    # Modify your INSERT statement to include the RECORD_TIMESTAMP
    cur.execute("""
        INSERT INTO AIR_QUALITY_DATA (
            LON, LAT, DATE, AQI, CO, NO, NO2, O3, SO2, PM2_5, PM10, NH3, RECORD_TIMESTAMP
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """, tuple(air_quality_data))

# Commit the transaction
conn.commit()


# Commit the transaction
conn.commit()

# Close the cursor and connection
cur.close()
conn.close()