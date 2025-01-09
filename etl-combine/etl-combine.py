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

def drop_columns_id(df: pd.DataFrame):
    df.drop(columns=["_id_x", "_id_y"], inplace=True, errors='ignore')
    return df

def change_city_name(df: pd.DataFrame):
    df["city"] = df["city"].replace("Ap Ba", "Da Nang")
    df["city"] = df["city"].replace("Hanoi", "Ha Noi")
    df["city"] = df["city"].str.lower()
    return df 

def split_time_to_hour_minute(df: pd.DataFrame):
    df["hour"] = df["time"].str.split(":").str[0].astype(int)
    df["minute"] = df["time"].str.split(":").str[1].astype(int)
    df.drop(columns=["time"], inplace=True)
    return df

def clean_data(df_input: pd.DataFrame):
    df = df_input.copy()
    # Drop duplicates
    df.drop_duplicates(inplace=True)
    
    # Drop rows with missing values
    df.dropna(inplace=True)

    return df

def merge_data(air_clean: pd.DataFrame, traffic_clean: pd.DataFrame, weather_clean: pd.DataFrame):
    # Merge data
    df_output = weather_clean.merge(air_clean, on=["date", "city", "hour", "minute"], how="inner") \
                                .merge(traffic_clean, on=["date", "city", "hour", "minute"], how="inner")

    return df_output

import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

def classify_pollution(df, n_clusters=3, random_state=42):
    # Select relevant features
    features = ['humidity', 'pressure', 'temperature', 'wind_speed','magnitudeOfDelay', 'pm10', 'pm2_5', 'co', 'so2', 'o3']
    X = df[features]

    # Handle missing values
    X = X.fillna(X.mean())

    # Standardize the data
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # Apply K-Means clustering
    kmeans = KMeans(n_clusters=n_clusters, random_state=random_state)
    df['pollution_cluster'] = kmeans.fit_predict(X_scaled)

    # Inverse transform centroids for interpretation
    centroids = scaler.inverse_transform(kmeans.cluster_centers_)
    centroid_df = pd.DataFrame(centroids, columns=features)

    # Map cluster IDs to pollution levels (customize mapping as needed)
    pollution_mapping = {0: 'High', 1: 'Moderate', 2: 'Low'}  # Adjust based on cluster analysis
    df['pollution_level'] = df['pollution_cluster'].map(pollution_mapping)
    
    df.drop(columns=['pollution_cluster'], inplace=True)
    return df


def transform_data(air_clean: pd.DataFrame, traffic_clean: pd.DataFrame, weather_clean: pd.DataFrame):
    
    

    # Change city name
    air_clean = change_city_name(air_clean)
    traffic_clean = change_city_name(traffic_clean)
    weather_clean = change_city_name(weather_clean)

    # Split time to hour and minute
    air_clean = split_time_to_hour_minute(air_clean)
    traffic_clean = split_time_to_hour_minute(traffic_clean)
    weather_clean = split_time_to_hour_minute(weather_clean)

    # Merge data
    df_output = merge_data(air_clean, traffic_clean, weather_clean)

    # Drop columns id
    df_output = drop_columns_id(df_output)
    
    # Classify pollution levels
    df_output = classify_pollution(df_output)

    # Clean the data
    df_output = clean_data(df_output)

    return df_output



def run():
    while True:
        data_air_clean = fetch_db('air_clean')
        data_traffic_clean = fetch_db('traffic_clean')
        data_weather_clean = fetch_db('weather_clean')
        
        # Transform data
        if data_air_clean is not None and data_traffic_clean is not None and data_weather_clean is not None:
            data_combine_cleaned = transform_data(data_air_clean, data_traffic_clean, data_weather_clean)
            
            # Upsert data to the cleaned collection
            upsert_db('combine_clean', data_combine_cleaned.to_dict(orient='records'))

            # Sleep for 1 hours
            time.sleep(3600)

if __name__ == "__main__":
    run()