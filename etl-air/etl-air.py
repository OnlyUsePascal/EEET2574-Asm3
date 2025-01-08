import pymongo
# from geopy.geocoders import Nominatim
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
#   return list(data)


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
    df['city'] = df['location'].apply(lambda x: x.get('name') if isinstance(x, dict) else None)
    df['uv'] = df['current'].apply(lambda x: x.get('uv') if isinstance(x, dict) else None)
    df['co'] = df['current'].apply(lambda x: x['air_quality'].get('co') if isinstance(x, dict) and 'air_quality' in x else None)
    df['o3'] = df['current'].apply(lambda x: x['air_quality'].get('o3') if isinstance(x, dict) and 'air_quality' in x else None)
    df['so2'] = df['current'].apply(lambda x: x['air_quality'].get('so2') if isinstance(x, dict) and 'air_quality' in x else None)
    df['pm2_5'] = df['current'].apply(lambda x: x['air_quality'].get('pm2_5') if isinstance(x, dict) and 'air_quality' in x else None)
    df['pm10'] = df['current'].apply(lambda x: x['air_quality'].get('pm10') if isinstance(x, dict) and 'air_quality' in x else None)
    df['us-epa-index'] = df['current'].apply(lambda x: x['air_quality'].get('us-epa-index') if isinstance(x, dict) and 'air_quality' in x else None)
    df['gb-defra-index'] = df['current'].apply(lambda x: x['air_quality'].get('gb-defra-index') if isinstance(x, dict) and 'air_quality' in x else None)

    # Drop parsed columns
    df.drop(columns=json_columns, inplace=True, errors='ignore')

    return df

def split_report_time(df, column_name='report_time'):
    # Ensure the column is in datetime format
    df[column_name] = pd.to_datetime(df[column_name])
    
    # Create new columns for date and time as strings
    df['date'] = df[column_name].dt.date.astype(str)
    df['time'] = df[column_name].dt.time.astype(str)
    
    # Optionally drop the original report_time column if no longer needed
    df = df.drop(columns=[column_name])
    
    return df

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
    df_output = parse_json_column(df_output)

    # Split report_time into date and time
    df_output = split_report_time(df_output)

    # Clean the data
    df_output = clean_data(df_output)

    return df_output

def run():
    while True:
        data_air_raw = fetch_db('air_raw')
        
        # Transform data
        if data_air_raw is not None:
            data_air_cleaned = transform_data(data_air_raw)
            
            # Upsert data to the cleaned collection
            upsert_db('air_clean', data_air_cleaned.to_dict(orient='records'))

        # Sleep for 1 hours
        time.sleep(3600)

if __name__ == "__main__":
    run()