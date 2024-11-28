import pandas as pd
import re
import boto3
from dotenv import dotenv_values
import pathlib
import numpy as np
script_path = pathlib.Path(__file__).parent.resolve()

config = dotenv_values(f'{script_path}/.env')

print(dict(config))

access_key = config['S3_ACCESS_KEY']
secret_key = config['S3_SECRET_KEY']
bucket_name = config['S3_BUCKET_NAME']

endpoint_url = 'http://minio-minio-1:9000'

# Initialize client
s3 = boto3.client('s3', 
                  endpoint_url=endpoint_url, 
                  aws_access_key_id=access_key,
                  aws_secret_access_key=secret_key)
file_keys = ['headphones-bronze.csv', 'iems-bronze.csv']

def get_data(file_key):   # return DataFrame
    try:
        # Get object
        obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        
        # Read data and load to dataframe
        df = pd.read_csv(obj['Body'])

        return df
    except Exception as e:
        print(f'Error when reading file from S3: {e}')



def clean_data(df):
    cols_to_choose = ['rank', 'value_rating', 'model', 'price_msrp', 'signature', 'tone_grade', 'technical_grade', 'driver_type']
    df = df[cols_to_choose]
    df['signature'] = df['signature'].str.replace('"', '')

    for index, row in df.iterrows():
        # Replacing discontinued devices with no price with -1
        if re.search('Discont', str(row['price_msrp'])):
            df.at[index, 'price_msrp'] = -1

        # Replacing ? device prices with -1
        if re.search('\\?', str(row['price_msrp'])):
            df.at[index, 'price_msrp'] = -1
        if row['driver_type'] == '?':
            df.at[index, 'driver_type'] = ''
        # Some prices have models embedded to them, this replaces with only price
        # Ex: 3000(HE1000) gives 3000
        if re.search('[a-zA-Z]', str(row['price_msrp'])):
            price_only = list(filter(None, re.split(r'(\d+)', str(row['price_msrp']))))[0]
            df.at[index, 'price_msrp'] = price_only

            # Some are still text even after splits and earlier cleanses
            if re.search('[a-zA-Z]', str(df.at[index, 'price_msrp'])):
                df.at[index, 'price_msrp'] = -1

        # Replace star text rating with number. If no stars, replace with -1
        df.at[index, 'value_rating'] = len(row['value_rating']) if row['value_rating'] and type(row['value_rating']) == str else -1
    df.replace('', np.nan, inplace=True)
    df.dropna(inplace = True)

    return df


def write_data(df, file_name, data_level):
    df.to_csv(f'/opt/airflow/data/{file_name}-{data_level}.csv', index=False)

if __name__ == '__main__':
    for file_key in file_keys:
        df = get_data(file_key)
        df = clean_data(df)
        file_name = file_key.split('-')[0]
        write_data(df, file_name, 'silver')