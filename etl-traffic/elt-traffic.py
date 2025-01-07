import pymongo
from geopy.geocoders import Nominatim
import pandas as pd
import numpy as np
import os
import seaborn as sns
from numpy import float64, int64
import numpy
from dateutil import parser
from datetime import datetime


MONGO_URL="mongodb+srv://viphilongnguyen:egVQ0C3HhJRuVYaZ@cluster0.khgwh.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = pymongo.MongoClient(MONGO_URL)
db = client.get_database('ASM3')

# ====== REQUEST AREA ======
def fetch_db(collection_name = ''):
  collection = db[collection_name]
  data = collection.find()
  # return pd.DataFrame(list(data))
  return list(data)


def upsert_db(collection_name = '', documents = []):
  upsertReqs = []
  for document in documents:
    upsertReqs.append(
      pymongo.UpdateOne(
        {'_id': document['_id']},
        {'$set': document},
        upsert=True
      )
    )
  
  collection = db[collection_name]
  collection.bulk_write(upsertReqs)


def delete_db(collectionName = '', documents = []):
    delReqs = []
    for document in documents:
        delReqs.append(
            pymongo.DeleteOne(
                {'_id': document['_id']}
            )
        )
    
    collection = db[collectionName]
    collection.bulk_write(delReqs)

# ====== TRANSFORM AREA ======
def data_merge_key(data_input):
  # bring child keys to same level as parent keys
  data_output = data_input
  new_keys = []

  for key_par, val_par in data_output[0].items():
    # print('>', key_par, type(val_par))
    if type(val_par) == dict:
      for key_chi, val_chi in val_par.items():
        # print(key_chi, type(val_chi))
        new_keys.append((key_par, key_chi))
  
  # merge key 
  for key_par, key_chi in new_keys:
    for data in data_output:
      data[key_par + '_' + key_chi] = data[key_par][key_chi]   
  
  # delete old key
  for data in data_output:
    for key, val in list(data.items()):
      if type(val) == dict:
        data.pop(key, None)

  return pd.DataFrame(data_output)


# drop columns before combine combine with other data
drop_columns = [
  'type',
  'geometry_type', 
  'properties_id', 
  'properties_roadNumbers',
  'properties_timeValidity',
  'properties_probabilityOfOccurrence',
  'properties_numberOfReports',
  'properties_lastReportTime',
  'properties_tmc',
  'properties_from',
  'properties_to',
]


def data_extract_events(df_input : pd.DataFrame):
  df_output = df_input.copy()

  df_output['event_code'] = df_output['properties_events'].apply(lambda x: x[0]['code'])
  df_output['event_desc'] = df_output['properties_events'].apply(lambda x: x[0]['description'])
  df_output.drop('properties_events', axis=1, inplace=True)

  # display(df_output.head(5))
  return df_output


def iso_to_local(iso_timestamp):
  if iso_timestamp == None:
    return None

  # Convert to local time
  utc_time = parser.isoparse(iso_timestamp)
  local_time = utc_time.astimezone()
  # print(f"Local time: {local_time}")
  return local_time


def data_convert_time(df_input):
  df_output = df_input.copy()
  df_output['properties_startTime'] = df_output['properties_startTime'].apply(iso_to_local)
  df_output['properties_endTime'] = df_output['properties_endTime'].apply(iso_to_local)

  # drop if none
  df_output.dropna(subset=['properties_startTime', 'properties_endTime'], inplace=True)
  return df_output



geo_cache = {}
def coordinate_to_address(row):
  coordinates = (row[1], row[0])
  
  # caching coordinate reverse
  location = geo_cache.get(coordinates)
  if location == None:
    geolocator = Nominatim(user_agent="etl-traffic")
    location = geolocator.reverse(
      coordinates,exactly_one=True, language="en", namedetails=True, addressdetails=True
    ).raw['address']
    geo_cache[coordinates] = location
  print(location)

  # extract city & district
  city = location.get('city')
  city_prefixes = ['City ', ' City']
  if city != None:
    for prefix in city_prefixes:
      city = city.replace(prefix, '') 

  district_keys = ['city_district', 'suburb']
  district_prefixes = ['District ', ' District']
  district = ''
  for key in district_keys:
    district = location.get(key)
    if district != None:
      for prefix in district_prefixes:
        district = district.replace(prefix, '')
      break
  
  return (district, city)


def data_extract_geometry(df_input : pd.DataFrame):
  df_output = df_input.copy()

  # get coordinate
  df_output['geometry_coordinates'] = df_output['geometry_coordinates'].apply(lambda x: [x[0][0], x[0][1]])
  
  for index, row in df_output.iterrows():
    # extract district and city
    address = coordinate_to_address(row['geometry_coordinates']) 
    (district, city) = address
    # print(address)

    df_output.loc[index, 'geometry_district'] = district
    df_output.loc[index, 'geometry_city'] = city
  
  df_output.drop('geometry_coordinates', axis=1, inplace=True)
  df_output.dropna(subset=['geometry_city', 'geometry_district'], inplace=True)
  return df_output


def run():
  data_traffic_raw = fetch_db('traffic_raw')
  df_merge_key = data_merge_key(data_traffic_raw)
  df_drop_columns = df_merge_key.drop(drop_columns, axis=1)
  df_extract_events = data_extract_events(df_drop_columns)
  df_convert_time = data_convert_time(df_extract_events)
  df_extract_geometry = data_extract_geometry(df_convert_time)
  upsert_db

if __name__ == "__main__":
  run()
  
  
  
  
  
  
