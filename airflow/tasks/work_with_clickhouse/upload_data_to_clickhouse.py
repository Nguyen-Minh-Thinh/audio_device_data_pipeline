import clickhouse_connect
import boto3
import pathlib
from dotenv import dotenv_values
import pandas as pd
script_path = pathlib.Path(__file__).parent.resolve()
config = dotenv_values(f'{script_path.parent.parent}/.env')
print(config)
try:
    # Initialize client 
    client = clickhouse_connect.get_client(host='host.docker.internal', username='default', port=8123)
    
except Exception as e:
    print(f"Error when connecting to clickhouse: {e}")

# Initialize client for using s3
access_key = config['S3_ACCESS_KEY']
secret_key = config['S3_SECRET_KEY']
bucket_name = config['S3_BUCKET_NAME']
endpoint_url = 'http://minio-minio-1:9000'

s3 = boto3.client('s3',
                  aws_access_key_id=access_key,
                  aws_secret_access_key=secret_key,
                  endpoint_url=endpoint_url)

def get_data_from_s3(file_key):
    try:
        # Get object
        obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        df = pd.read_csv(obj['Body'])
        
        return df
    except Exception as e:
        print(f'Error when reading file from S3: {e}')

def load_data(df, name_of_table):
    create_database = 'create database if not exists audio_device_data_pipeline'
    use_database = 'use audio_device_data_pipeline'
    drop_table = f'drop table if exists {name_of_table}'
    create_table = f'''
        create table if not exists {name_of_table} (
            rank_of_product String,
            value_rating Int8,
            model String,
            price_msrp Decimal(10, 2),
            signature String,
            tone_grade String,
            technical_grade String,
            driver_type String
        ) ENGINE = MergeTree()
        ORDER BY rank_of_product
    '''
    client.command(create_database)
    client.command(use_database)
    client.command(drop_table)
    client.command(create_table)
    for idx, row in df.iterrows():
        query = f'''
            INSERT INTO {name_of_table}
            (rank_of_product, value_rating, model, price_msrp, signature, tone_grade, technical_grade, driver_type)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        '''
        client.query(query, (row['rank'] if row['rank'] is not None else '',
                             row['value_rating'] if row['value_rating'] is not None else '',
                             row['model'] if row['model'] is not None else '',
                             row['price_msrp'] if row['price_msrp'] is not None else '',
                             row['signature'] if row['signature'] is not None else '',
                             row['tone_grade'] if row['tone_grade'] is not None else '',
                             row['technical_grade'] if row['technical_grade'] is not None else '',
                             row['driver_type'] if row['driver_type'] is not None else ''))


if __name__ == '__main__':
    file_keys = ['headphones-silver.csv', 'iems-silver.csv']
    for file_key in file_keys:
        df = get_data_from_s3(file_key)
        load_data(df, name_of_table=file_key.split('-')[0])
