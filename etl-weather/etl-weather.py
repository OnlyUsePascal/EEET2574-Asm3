import pymongo
import pandas as pd
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


def parse_json_column(df_input: pd.DataFrame):
    df = df_input.copy()

    # Extract fields from parsed JSON
    df['longitude'] = df['coord'].apply(lambda x: x.get('lon'))
    df['latitude'] = df['coord'].apply(lambda x: x.get('lat'))
    df['weather_main'] = df['weather'].apply(lambda x: x[0]['main'] if x else None)
    df['weather_desc'] = df['weather'].apply(lambda x: x[0]['description'] if x else None)
    df['temperature'] = df['main'].apply(lambda x: x.get('temp'))
    df['feels_like'] = df['main'].apply(lambda x: x.get('feels_like'))
    df['pressure'] = df['main'].apply(lambda x: x.get('pressure'))
    df['humidity'] = df['main'].apply(lambda x: x.get('humidity'))
    df['wind_speed'] = df['wind'].apply(lambda x: x.get('speed'))
    df['wind_deg'] = df['wind'].apply(lambda x: x.get('deg'))
    df['cloudiness'] = df['clouds'].apply(lambda x: x.get('all'))
    df['country'] = df['sys'].apply(lambda x: x.get('country'))
    df['sunrise'] = df['sys'].apply(lambda x: datetime.utcfromtimestamp(x.get('sunrise')))
    df['sunset'] = df['sys'].apply(lambda x: datetime.utcfromtimestamp(x.get('sunset')))
    
    json_columns = ['coord', 'weather', 'main', 'wind', 'clouds', 'sys']
    # Drop parsed columns
    df.drop(columns=json_columns, inplace=True)
    return df

def drop_columns(df_input: pd.DataFrame):
    df = df_input.copy()
    columns_to_drop = ['base', 'dt', 'cod', 'id', 'timezone', 'sunrise', 'sunset','country']
    df = df.drop(columns=columns_to_drop, errors='ignore')
    return df

def rename_columns(df_input: pd.DataFrame):
    df = df_input.copy()
    df.rename(columns={
        'name': 'city',
    }, inplace=True)
    return df

def split_report_time(df_input: pd.DataFrame, column_name='report_time'):
    df = df_input.copy()
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

    # Drop unnecessary columns
    df_output = drop_columns(df_output)

    # Rename columns
    df_output = rename_columns(df_output)

    # Split report_time into date and time
    df_output = split_report_time(df_output, 'report_time')

    # Clean data
    df_output = clean_data(df_output)

    return df_output

def run():
    while True: 
        # Extract raw data
        data_weather_raw = fetch_db('weather_raw')

        # Transform data
        if data_weather_raw is not None:
            df_transformed = transform_data(data_weather_raw)
            # Upsert cleaned data
            upsert_db('weather_clean', df_transformed.to_dict('records'))
        
        # Sleep for 1 hour
        time.sleep(3600)

if __name__ == "__main__":
    run()