import asyncio
import os
import time
import json  
from pymongo import MongoClient
import requests
import aiohttp
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
    air_collection = db['air_raw'] 
    return air_collection

# Real-time Air Quality key
access_token = os.getenv('AQICN_ACCESS_TOKEN')

# Connect to the Real-time Air Quality data feed
base_url = "https://api.waqi.info/feed/"

async def get_air(city):
    """Fetch the current air data from Real-time Air Quality data feed."""
    async with aiohttp.ClientSession() as session:
        url = f"{base_url}{city}/?token={access_token}"
        async with session.get(url) as response:
            if response.status == 200:
                air_data = await response.json()
                return air_data
            else:
                print(f"Error: {response.status} - {response.text()}")
                return None

def insert_to_mongo(air_collection, air_data):
    """Insert air data into MongoDB."""
    try:
        # Insert air data into the MongoDB collection
        air_collection.insert_one(air_data)
        print("Data inserted successfully.")
    except Exception as e:
        print(f"Error inserting data into MongoDB: {e}")

async def run(air_collection):
    """Main function to fetch and store air data in a loop."""
    cities = ["ho-chi-minh-city", "danang", "hanoi"]
    cities = [
        {"location": "Ho Chi Minh City", "url_city": "ho-chi-minh-city"},
        {"location": "Da Nang", "url_city": "danang"},
        {"location": "Ha Noi", "url_city": "hanoi"},
    ]
    iterator = 0
    cooldown = 1  

    while True:
        for city in cities:
            response = await get_air(city["url_city"])
            if response:
                # Add additional data like the location name and timestamp
                air_data = response["data"]
                air_data["report_time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                air_data["city"]["location"]=city["location"]
                print(air_data)

                # Insert the data into MongoDB
                insert_to_mongo(air_collection, air_data)

            # Sleep between location requests
            await asyncio.sleep(cooldown)

if __name__ == "__main__":
    air_collection = connect_to_db()
    asyncio.run(run(air_collection))
