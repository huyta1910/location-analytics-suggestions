import pandas as pd
from pymongo import MongoClient
import configparser
import clickhouse_connect

def extract_from_mongo(config):
    """Extracts data from the source MongoDB collection."""
    client = MongoClient(config.get('mongodb', 'uri'))
    db = client[config.get('mongodb', 'db_name')]
    collection = db[config.get('mongodb', 'source_collection')]
    
    print("--- Extracting from MongoDB ---")
    data = list(collection.find({}, {'_id': 0}))
    client.close()
    
    if not data:
        raise ValueError("Source collection is empty. Aborting.")
        
    df = pd.DataFrame(data)
    print(f"Extracted {len(df)} records.")
    return df

def load_to_clickhouse(df, config):
    """Loads a DataFrame into a ClickHouse table."""
    host = config.get('clickhouse', 'host')
    port = config.get('clickhouse', 'port')
    database = config.get('clickhouse', 'database')
    table = config.get('clickhouse', 'raw_places_table')

    client = clickhouse_connect.get_client(host=host, port=port)
    
    print(f"--- Loading to ClickHouse table: {database}.{table} ---")
    
    # Drop and recreate the table for a full refresh
    client.command(f'DROP TABLE IF EXISTS {database}.{table}')
    
    # Let clickhouse-connect create the table schema from the DataFrame
    client.insert_df(df, table_name=table, database=database)
    print(f"Successfully loaded {len(df)} records into ClickHouse.")

def run_extract_load():
    """Main function to run the EL process."""
    config = configparser.ConfigParser()
    config.read('/opt/airflow/pipeline_config/pipeline_config.ini')
    
    df = extract_from_mongo(config)
    load_to_clickhouse(df, config)
    print("--- EL Process Finished Successfully ---")
if __name__ == "__main__":
    run_extract_load()