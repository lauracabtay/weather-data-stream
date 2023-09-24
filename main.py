from confluent_kafka import Producer
from datetime import datetime as dt
from dotenv import load_dotenv
from pprint import pprint as pp
import json
import os
import requests
import time


load_dotenv()
API_KEY = os.getenv('API_KEY')

producer = Producer({'bootstrap.servers': 'localhost:9092'})


def get_locations_list() -> list:
    with open('data/locations.json', 'r') as locations_file:
        data = json.load(locations_file)
        locations = data['locations']

        return locations


def get_coordinates(locations) -> list:
    locations_data = []
    for location in locations:
        endpoint = f"http://api.openweathermap.org/geo/1.0/direct?q={location}&limit=1&appid={API_KEY}"

        response = requests.get(endpoint)
        data = response.json()

        locations_data.append(
            {
                "city": data[0]['name'] if 'local_names' not in location else data[0]['name']['en'],
                "country": data[0]['country'],
                "lat": data[0]['lat'],
                "lon": data[0]['lon']
            },
        )
    return locations_data


def get_weather(location_data) -> list:
    lat = location_data['lat']
    lon = location_data['lon']

    endpoint = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}"
    response = requests.get(endpoint)
    data = response.json()

    timestamp = dt.fromtimestamp(data['dt']).isoformat()

    weather_data.append(
        {
            "timestamp": timestamp,
            "country": data['sys']['country'],
            "city": data['name'],
            "weather": data['weather'][0]['main'],
            "temperature": data['main']['temp'],
            "wind_speed": data['wind']['speed'],
            "humidity": data['main']['humidity'],
            "pressure": data['main']['pressure']
        }
    )

    weather_data_as_json = json.dumps(weather_data[-1])

    current_timestamp = dt.now()
    producer.produce('weather-data', key=str(current_timestamp), value=weather_data_as_json)

    return weather_data


location_list = get_locations_list()
locations_coordinates = get_coordinates(location_list)

weather_data = []
while True:
    for location_coordinates in locations_coordinates:
        get_weather(location_coordinates)
    pp(weather_data)
    producer.flush()
    time.sleep(60)
