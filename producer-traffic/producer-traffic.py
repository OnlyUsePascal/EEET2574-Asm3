"""Produce openweathermap content to 'weather' kafka topic."""
import asyncio
import configparser
import os
import time
from collections import namedtuple
import datetime
from pymongo import MongoClient, UpdateOne

from dotenv import load_dotenv
import requests
from websocket import send
from dataprep.connector import connect

load_dotenv()
TOMTOM_KEY = os.getenv('TOMTOM_KEY')
CLUSTER_URL = os.getenv('CLUSTER_URL')

# print(TOMTOM_KEY, CLUSTER_URL)
# TOMTOM_KEY = 'mXoFdnZNt6QKMjsivV5TxQKG5BezGf9M'
# CLUSTER_URL = 'mongodb+srv://user1:123@cluster0.1xjq9.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0'

RAW_DB = 'asm3-test1'
RAW_COLL = 'traffic-raw'

dbClient = MongoClient(CLUSTER_URL)
collClient = dbClient\
    .get_database(RAW_DB)\
    .get_collection(RAW_COLL)
# TRAFFIC_COLLECTION = RAW_DB.traffic

def fetch_incidents(city_bbox):
    minLon, minLat, maxLon, maxLat = city_bbox
    url = (f"https://api.tomtom.com/traffic/services/5/incidentDetails"
           + f"?key={TOMTOM_KEY}"
           + "&fields={incidents{type,geometry{type,coordinates},properties{id,iconCategory,magnitudeOfDelay,events{description,code,iconCategory},startTime,endTime,from,to,length,delay,roadNumbers,timeValidity,probabilityOfOccurrence,numberOfReports,lastReportTime,tmc{countryCode,tableNumber,tableVersion,direction,points{location,offset}}}}}"
           + "&language=en-GB"
           + f"&bbox={minLon},{minLat},{maxLon},{maxLat}"
           + "&categoryFilter=1,2,4,6,8,9,10,11"
           # + "&fields={incidents{type,geometry{type,coordinates},properties{id,iconCategory,magnitudeOfDelay,events{description,code,iconCategory},startTime,endTime,from,to,length,delay,roadNumbers,timeValidity,probabilityOfOccurrence,numberOfReports,lastReportTime}}}"\
           )

    # print(url)
    res = requests.get(url)
    return res.json()["incidents"]

def upload_incidents(incidents_hist):
    update_requests = []
    # print(incidents_hist)
    
    # update incidents + upload new if not exists
    for incidents in incidents_hist:
        for incident in incidents:
            incident["_id"] = incident["properties"]["id"]
            update_requests.append(
                UpdateOne({"_id": incident["_id"]}, {"$set": incident}, upsert=True)
            )
    
    # write to db
    collClient.bulk_write(update_requests)

def run():
    incidents_hist = []
    cities_bbox = {
        "HCM City": [106.532129, 10.66594, 106.831575, 10.883411],
        "Hanoi": [105.2849, 20.5645, 106.0201, 21.3853],
        "Da Nang": [107.818782, 15.917955, 108.574858, 16.344307],
        # "Hai Phong": [106.4005, 20.2208, 107.1187, 21.0203],
        # "Can Tho": [105.225678, 9.919531, 105.845472, 10.325209],
    }
    cities = list(cities_bbox.keys())
    delay_fetch = 60 / len(cities)
    delay_upload = 60 * 15
    # delay_fetch = 5 
    # delay_upload = 10
    iterator = 0
    last_fetch = datetime.datetime.now()

    while True:
        try:
            city = cities[iterator]
            print(f'> Fetching incidents for {city}')
            incidents_hist.append(fetch_incidents(cities_bbox[city]))

            if (datetime.datetime.now() - last_fetch).seconds > delay_upload:
                print('> Uploading to mongo...')
                last_fetch = datetime.datetime.now()
                upload_incidents(incidents_hist)
                incidents_hist = []

        except Exception as err:
            print('> Something went wrong !')
            print(err)

        finally:
            time.sleep(delay_fetch)
            print("> Waking up!")
            iterator = (iterator + 1) % len(cities)


if __name__ == '__main__':
    run()