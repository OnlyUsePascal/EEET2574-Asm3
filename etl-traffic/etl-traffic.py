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
import time


MONGO_URL="mongodb+srv://viphilongnguyen:egVQ0C3HhJRuVYaZ@cluster0.khgwh.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = pymongo.MongoClient(MONGO_URL)
db = client.get_database('ASM3')

# ====== REQUEST AREA ======
def fetch_db(collection_name = ''):
  collection = db[collection_name]
  data = collection.find()
  return pd.DataFrame(list(data))
  # return list(data)


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

import pandas as pd

def parse_json_column(df, json_columns):
    """
    Parses specified JSON columns in a DataFrame and extracts required fields into new columns.

    Parameters:
    - df (pd.DataFrame): The input DataFrame.
    - json_columns (list): List of column names containing JSON data.

    Returns:
    - pd.DataFrame: The DataFrame with extracted fields as new columns and JSON columns dropped.
    """
    # Extract fields from parsed JSON
    df['city'] = df['properties'].apply(lambda x: x.get('city') if isinstance(x, dict) else None)
    df['iconCategory'] = df['properties'].apply(lambda x: x.get('iconCategory') if isinstance(x, dict) else None)
    df['magnitudeOfDelay'] = df['properties'].apply(lambda x: x.get('magnitudeOfDelay') if isinstance(x, dict) else None)
    df['startTime'] = df['properties'].apply(lambda x: x.get('startTime') if isinstance(x, dict) else None)
    df['endTime'] = df['properties'].apply(lambda x: x.get('endTime') if isinstance(x, dict) else None)
    df['length'] = df['properties'].apply(lambda x: x.get('length') if isinstance(x, dict) else None)
    df['delay'] = df['properties'].apply(lambda x: x.get('delay') if isinstance(x, dict) else None)
    # get first element in event array
    event = df['properties'].apply(lambda x: x.get('events')[0] if isinstance(x, dict) and 'events' in x and len(x['events']) > 0 else None)
    df['event_code'] = event.apply(lambda x: x.get('code') if isinstance(x, dict) else None)
    df['event_desc'] = event.apply(lambda x: x.get('description') if isinstance(x, dict) else None)

    # Drop parsed columns
    df.drop(columns=json_columns, inplace=True, errors='ignore')

    return df

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
  df_output['reportTime'] = df_output['startTime'].apply(iso_to_local)
  df_output['reportTime'] = pd.to_datetime(df_output['startTime']).dt.tz_localize(None)
  df_output.drop(columns=['startTime', 'endTime'], inplace=True, errors='ignore')

  return df_output

def clean_data(df_input: pd.DataFrame):
    df = df_input.copy()
    # Drop duplicates
    df.drop_duplicates(inplace=True)
    
    # Drop rows with missing values
    df.dropna(inplace=True)

    return df

def transform_data(df_input: pd.DataFrame):
    df_output = df_input.copy()

    # Parse JSON columns
    df_output = parse_json_column(df_output, json_columns =['geometry', 'properties', 'type'])

    # Convert time to local time
    df_output = data_convert_time(df_output)

    # Clean the data
    df_output = clean_data(df_output)

    return df_output


def run():
  while True:
    data_traffic_raw = fetch_db('traffic_raw')

    # Transform data
    if data_traffic_raw is not None:
      data_traffic_cleaned = transform_data(data_traffic_raw)

      upsert_db('traffic_clean', data_traffic_cleaned.to_dict('records'))


    # Sleep for 1 hours
    time.sleep(3600)

  



if __name__ == "__main__":
  run()
  
  
  
  
  
  
