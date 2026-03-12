import pandas as pd
import numpy as np
import os
import time
import fastavro
import json

# Define Avro Schema
schema = {
    'doc': 'GlobalShop Transactions',
    'name': 'Transaction',
    'namespace': 'quickride',
    'type': 'record',
    'fields': [
        {'name': 'transaction_id', 'type': 'long'},
        {'name': 'user_id', 'type': 'int'},
        {'name': 'amount', 'type': 'double'},
        {'name': 'timestamp', 'type': {'type': 'long', 'logicalType': 'timestamp-micros'}},
        {'name': 'category', 'type': 'string'},
        {'name': 'is_fraud', 'type': 'boolean'},
        {'name': 'description', 'type': 'string'},
    ],
}

# Parse Avro schema for use with fastavro
parsed_schema = fastavro.parse_schema(schema)

def save_avro(df, filename='data.avro', codec="null"):
    # Convert DataFrame to dict list (records)
    records = df.to_dict('records')
    
    with open(filename, 'wb') as out:
        fastavro.writer(out, parsed_schema, records, codec=codec)

def generate_dataset(n_rows=1000000):
    print(f"Generating dataset with {n_rows} records...")
    data = {
        'transaction_id': np.arange(n_rows),
        'user_id': np.random.randint(1000, 9999, size=n_rows),
        'amount': np.random.uniform(1.0, 5000.0, size=n_rows),
        'timestamp': pd.date_range(start='2024-01-01', periods=n_rows, freq='s'),
        'category': np.random.choice(['Retail', 'Food', 'Travel', 'Health', 'Tech'], size=n_rows),
        'is_fraud': np.random.choice([True, False], size=n_rows, p=[0.01, 0.99]),
        'description': ['A' * 50 for _ in range(n_rows)] # Repetitive text for compression testing
    }
    return pd.DataFrame(data)

df = generate_dataset()

folder = 'temporal_files/'
os.makedirs(folder, exist_ok=True)

formats_config = {
    'CSV': {'filename': 'data.csv', 'func': lambda df, p: df.to_csv(p, index=False)},
    'JSON': {'filename': 'data.json', 'func': lambda df, p: df.to_json(p, orient='records', lines=True, date_format='iso')},
    'Parquet': {'filename': 'data.parquet', 'func': lambda df, p: df.to_parquet(p, compression=None)},
    'Parquet (Snappy)': {'filename': 'data_snappy.parquet', 'func': lambda df, p: df.to_parquet(p, compression='snappy')},
    'Parquet (Gzip)': {'filename': 'data_gzip.parquet', 'func': lambda df, p: df.to_parquet(p, compression='gzip')},
    'Avro': {'filename': 'data_avro.avro', 'func': lambda df, p: save_avro(df, p)},
    'Avro (Snappy)': {'filename': 'data_avro_snappy.avro', 'func': lambda df, p: save_avro(df, p, codec='snappy')},
    'Avro (Deflate)': {'filename': 'data_avro_deflate.avro', 'func': lambda df, p: save_avro(df, p, codec='deflate')},
}

def get_file_size_mb(file_path):
    # Get file size in bytes
    size_in_bytes = os.path.getsize(file_path) #
    
    # Convert bytes to megabytes (1 MB = 1024 * 1024 bytes)
    size_in_mb = size_in_bytes / (1024 * 1024) #
    
    return size_in_mb

# Helper function to define the reading logic
def get_read_func(name):
    if 'Parquet' in name:
        return lambda p, col: pd.read_parquet(p, columns=[col])
    elif name == 'CSV':
        return lambda p, col: pd.read_csv(p, usecols=[col])
    elif name == 'JSON':
        return lambda p, col: pd.read_json(p, lines=True)[[col]]
    elif 'Avro' in name:
        return lambda p, col: [r[col] for r in fastavro.reader(open(p, 'rb'))]
    return lambda p, col: None

# Helper function to define the "read all" logic
def get_read_all_func(name):
    if 'Parquet' in name:
        return lambda p: pd.read_parquet(p)
    elif name == 'CSV':
        return lambda p: pd.read_csv(p)
    elif name == 'JSON':
        return lambda p: pd.read_json(p, lines=True)
    elif 'Avro' in name:
        return lambda p: list(fastavro.reader(open(p, 'rb'))) # only generate list of dicts for Avro to avoid overhead of converting to DataFrame
    return lambda p: None

results = []

for name, config in formats_config.items():
    file_path = folder + config['filename']

    # Write test
    write_start_time = time.time()
    config['func'](df, file_path)
    write_duration = time.time() - write_start_time

    # Get file size in MB
    file_size = get_file_size_mb(file_path)

    # Read one column test 
    read_func = get_read_func(name)
    start = time.time()
    read_func(file_path, 'amount')
    read_duration = time.time() - start

    # Read all columns test
    read_all_func = get_read_all_func(name)
    start = time.time()
    read_all_func(file_path)
    read_all_duration = time.time() - start

    # Format results
    results.append({'Format': name, 
                    'Write Time (s)': round(write_duration, 3), 
                    'Read one column Time (s)': round(read_duration, 3), 
                    'Read all columns Time (s)': round(read_all_duration, 3),
                    'Size (MB)': round(file_size, 2)
                    })
                    

df_results = pd.DataFrame(results)
print(df_results)
