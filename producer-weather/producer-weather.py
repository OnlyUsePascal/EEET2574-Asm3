import asyncio
import os
import time
import json  
from pymongo import MongoClient
from dataprep.connector import connect
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# MongoDB URI from environment variable
uri = os.getenv("MONGO_URI")

# Function to connect to MongoDB
def connect_to_db():
    client = MongoClient(uri)
    print("Connected to MongoDB")
    db = client['ASM3'] 
    weather_collection = db['weather_data'] 
    return weather_collection

# OpenWeather API key
access_token = os.getenv('OPENWEATHER_ACCESS_TOKEN')

# Connect to the OpenWeather API
sc = connect('openweathermap', _auth={'access_token': access_token}, _concurrency=3)

async def get_weather(city):
    """Fetch the current weather data from the OpenWeather API."""
    df_weather = await sc.query("weather", q=city)
    return df_weather

def insert_to_mongo(weather_collection, weather_data):
    """Insert weather data into MongoDB."""
    try:
        # Insert weather data into the MongoDB collection
        weather_collection.insert_one(weather_data)
        print("Data inserted successfully.")
    except Exception as e:
        print(f"Error inserting data into MongoDB: {e}")

def run(weather_collection):
    """Main function to fetch and store weather data in a loop."""
    locations = ["Ho Chi Minh", "Melbourne"]
    iterator = 0
    cooldown = 1  

    while True:
        try:
            # Fetch weather data
            location = locations[(iterator + 1) % len(locations)]
            current_weather = asyncio.run(get_weather(city=location))
            current_weather['location'] = location
            now = time.localtime()
            current_weather['report_time'] = time.strftime("%Y-%m-%d %H:%M:%S", now)
            current_weather = json.loads(current_weather.to_json(orient="records"))
            sendit = current_weather[0]  

            # Store the fetched data
            insert_to_mongo(weather_collection, sendit)

        except Exception as err:
            print(f"Something went wrong while fetching data for {location}!")
            print(err)

        finally:
            print(f"Finished processing {location}. Sleeping for {cooldown} seconds.")
            time.sleep(cooldown)
            iterator += 1

if __name__ == "__main__":
    weather_collection = connect_to_db()
    run(weather_collection)
