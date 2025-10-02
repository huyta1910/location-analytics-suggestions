import configparser
import pandas as pd
from pymongo import MongoClient
import clickhouse_connect


def extract_and_load(config, source_collection_key, target_table_key):
    """
    Extracts data from a single MongoDB collection and loads it into a specific ClickHouse table.
    """
    client = MongoClient(config.get('mongodb', 'uri'))
    db = client[config.get('mongodb', 'db_name')]
    collection_name = config.get('mongodb', source_collection_key)
    collection = db[collection_name]
    print(f"\n--- Extracting from MongoDB collection: '{collection_name}' ---")
    
    data = list(collection.find({}, {'_id': 0}))
    client.close()

    if not data:
        print(f"Warning: Collection '{collection_name}' is empty. Skipping.")
        return

    df = pd.json_normalize(data)
    print(f"Extracted {len(df)} records.")

    # connect to ClickHouse
    host = config.get('clickhouse', 'host')
    port = config.get('clickhouse', 'port')
    database = config.get('clickhouse', 'database')
    table_name = config.get('clickhouse', target_table_key)
    full_table_name = f"{database}.{table_name}"
    
    ch_client = clickhouse_connect.get_client(host=host, port=port)
    print(f"--- Loading to ClickHouse table: {full_table_name} ---")


    print("Cleaning DataFrame before insertion...")
    for col in df.columns:
        # Check if a column contains lists, indicating it's an Array type
        if any(isinstance(val, list) for val in df[col].dropna()):
            print(f"  - Cleaning list-like column: '{col}'")
            # For list-like columns, fill missing values with an empty list
            df[col] = df[col].apply(lambda x: x if isinstance(x, list) else [])
        # Check if a column is of 'object' dtype (likely a string)
        elif df[col].dtype == 'object':
            print(f"  - Cleaning string-like column: '{col}'")
            # For string-like columns, fill missing values with an empty string
            df[col] = df[col].fillna('')
    
    # Drop, Create, Insert Logic
    ch_client.command(f'DROP TABLE IF EXISTS {full_table_name}')
    create_sql = generate_create_table_sql(df, full_table_name)
    ch_client.command(create_sql)
    
    # Rename columns after creating the schema but before inserting
    df.columns = [col.replace('.', '_') for col in df.columns]
    
    ch_client.insert_df(full_table_name, df)
    print(f"Successfully loaded {len(df)} records into {full_table_name}.")

# The generate_create_table_sql function remains the same
def generate_create_table_sql(df: pd.DataFrame, table_name: str) -> str:
    dtype_mapping = {'int64': 'Int64', 'float64': 'Float64', 'bool': 'UInt8', 'datetime64[ns]': 'DateTime64', 'object': 'String'}
    cols_with_types = []
    for col_name, dtype in df.dtypes.items():
        safe_col_name = col_name.replace('.', '_')
        if any(isinstance(val, list) for val in df[col_name].dropna()):
            ch_type = 'Array(String)'
        else:
            base_type = dtype_mapping.get(str(dtype), 'String')
            ch_type = f'Nullable({base_type})'
        cols_with_types.append(f"`{safe_col_name}` {ch_type}")
    create_statement = (f"CREATE TABLE {table_name} (" f"{', '.join(cols_with_types)}" f") ENGINE = MergeTree() ORDER BY tuple()")
    return create_statement

def run_local_test():
    """Main function to run the EL process for local testing."""
    print("--- Starting LOCAL Extract-Load Test ---")
    config = configparser.ConfigParser()
    config.read('config/pipeline_config.ini')

    if not config.sections():
        raise FileNotFoundError("Could not find or read 'config/pipeline_config.ini'.")

    print("--- Overriding config for local environment (connecting to localhost) ---")
    config.set('mongodb', 'uri', 'mongodb://localhost:27017/')
    config.set('clickhouse', 'host', 'localhost')
    
    try:
        extract_and_load(config, 'location_collection', 'raw_locations_table')
        
        extract_and_load(config, 'restaurant_collection', 'raw_restaurants_table')
        
        print("\n--- LOCAL TEST FINISHED SUCCESSFULLY ---")
    except Exception as e:
        print(f"\n--- LOCAL TEST FAILED ---")
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    run_local_test()