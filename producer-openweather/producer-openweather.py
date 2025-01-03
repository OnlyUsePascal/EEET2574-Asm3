"""Produce openweathermap content to 'weather' kafka topic."""
import asyncio
import configparser
import os
import time
from collections import namedtuple

from dotenv import load_dotenv
from websocket import send
from dataprep.connector import connect

load_dotenv()

access_token = os.getenv('OPENWEATHER_ACCESS_TOKEN')

sc = connect('openweathermap',
             _auth={'access_token': access_token},
             _concurrency=3)

async def get_weather(city):
    # Get the current weather details for the given city.
    df_weather = await sc.query("weather", q=city)
    return df_weather

def run():
    locations = ["Ho Chi Minh", "Melbourne"]
    iterator = 0
    cooldown = 10

    while True:
        try:
            location = locations[(iterator + 1) % len(locations)]
            current_weather = asyncio.run(get_weather(city=location))
            current_weather['location'] = location
            now = time.localtime()
            current_weather['report_time'] = time.strftime("%Y-%m-%d %H:%M:%S", now)
            current_weather = current_weather.to_json(orient="records")
            sendit = current_weather[1:-1]
            print(sendit)

        except Exception as err:
            print('Something went wrong !')
            print(err)

        finally:
            time.sleep(cooldown)
            print("Waking up!")
            iterator += 1


if __name__ == "__main__":
    run()