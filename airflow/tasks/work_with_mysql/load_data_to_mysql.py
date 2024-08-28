import mysql.connector
import boto3
import pandas as pd
import pathlib
from dotenv import dotenv_values

script_path = pathlib.Path(__file__).parent.resolve()
config = dotenv_values(f'{script_path.parent.parent}/.env')
# Initialize a variable to hold the MySQL database connection
conn = None
try:
    mysql_host = config['MYSQL_HOST']
    mysql_user = config['MYSQL_USER']
    mysql_password = config['MYSQL_PASSWORD']
    # Establish a connection to the MySQL database
    conn = mysql.connector.connect(host=mysql_host, 
                                   port=3307, 
                                   user=mysql_user, 
                                   password=mysql_password)
    if conn.is_connected():
        print('Connected to MySQL database')
except mysql.connector.Error as e:
    # Print an error message if a connection error occurs
    print(e)

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
    cursor = conn.cursor()
    create_database = 'create database if not exists audio_device_data_pipeline'
    use_data_base = 'use audio_device_data_pipeline'
    drop_table = f'''drop table if exists {name_of_table}
                    '''
    create_table = f'''create table if not exists {name_of_table}(
                    rank_of_product varchar(5),
                    value_rating tinyint,
                    model varchar(255),
                    price_msrp decimal(10, 2),
                    signature varchar(50),
                    tone_grade varchar(5),
                    technical_grade varchar(5),
                    driver_type varchar(20)
                    )'''

    cursor.execute(create_database)
    cursor.execute(use_data_base)
    cursor.execute(drop_table)
    cursor.execute(create_table)

    for idx, row in df.iterrows():
        query = f'''
                insert into {name_of_table}
                values (%s, %s, %s, %s, %s, %s, %s, %s)
                '''
        cursor.execute(query, (row['rank'] if row['rank'] is not None else '',
        row['value_rating'] if row['value_rating'] is not None else '',
        row['model'] if row['model'] is not None else '',
        row['price_msrp'] if row['price_msrp'] is not None else '',
        row['signature'] if row['signature'] is not None else '',
        row['tone_grade'] if row['tone_grade'] is not None else '',
        row['technical_grade'] if row['technical_grade'] is not None else '',
        row['driver_type'] if row['driver_type'] is not None else ''))
    conn.commit()
    cursor.close


if __name__ == '__main__':
    file_keys = ['headphones-silver.csv', 'iems-silver.csv']
    for file_key in file_keys:
        df = get_data_from_s3(file_key)
        load_data(df, name_of_table=file_key.split('-')[0])
    # Close the database connection 
    if conn is not None and conn.is_connected():
        conn.close()