import asyncio
import os
import time
import json  
from pymongo import MongoClient
import requests
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
    weather_collection = db['weather_raw'] 
    return weather_collection

# OpenWeather API key
access_token = os.getenv('OPENWEATHER_ACCESS_TOKEN')

# Connect to the OpenWeather API
base_url = "https://api.openweathermap.org/data/2.5/weather"

async def get_weather(lat, lon):
    """Fetch the current weather data from the OpenWeather API."""
    response = requests.get(base_url, params={
        "lat": lat,
        "lon": lon,
        "appid": access_token
    })

    # Check if the request was successful
    if response.status_code == 200:
        weather_data = response.json()
        return weather_data
    else:
        print(f"Error: {response.status_code} - {await response.text}")
        return None

def insert_to_mongo(weather_collection, weather_data):
    """Insert weather data into MongoDB."""
    try:
        # Insert weather data into the MongoDB collection
        weather_collection.insert_one(weather_data)
        print("Data inserted successfully.")
    except Exception as e:
        print(f"Error inserting data into MongoDB: {e}")

async def run(weather_collection):
    """Main function to fetch and store weather data in a loop."""
    locations = [
        {"name": "Ho Chi Minh", "lat": 10.762622, "lon": 106.660172},
        {"name": "Da Nang", "lat": 16.047079, "lon": 108.206230},
        {"name": "Ha Noi", "lat": 21.028511, "lon": 105.804817},
    ]
    cooldown = 15  

    while True:
        for location in locations:
            weather_data = await get_weather(location["lat"], location["lon"])
            if weather_data:
                # Add additional data like the location name and timestamp
                weather_data["report_time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                print(weather_data)

                # Insert the data into MongoDB
                insert_to_mongo(weather_collection, weather_data)

            # Sleep between location requests
            await asyncio.sleep(cooldown)

if __name__ == "__main__":
    weather_collection = connect_to_db()
    asyncio.run(run(weather_collection))