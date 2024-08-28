import boto3
from botocore.exceptions import NoCredentialsError
from dotenv import dotenv_values
import pathlib
import sys
'''
    Load environment variables from .env file
'''
# Get the directory path absolutely that contains this file
script_path = pathlib.Path(__file__).parent.resolve()   
# .parent to get the parent directory of its current directory
config = dotenv_values(f'{script_path.parent.parent}/.env')
# print(script_path.parent)
# print(config)

data_level = sys.argv[1]

# Access environment variables
access_key = config['S3_ACCESS_KEY']
secret_key = config['S3_SECRET_KEY']
bucket_name = config['S3_BUCKET_NAME']

files = [f'/opt/airflow/data/headphones-{data_level}.csv', f'/opt/airflow/data/iems-{data_level}.csv']
endpoint_url = 'http://minio-minio-1:9000' # Minio

try:
    # Initialize client of S3
    s3_client = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=endpoint_url
    )

    # Upload file
    for i in range(len(files)):
        file_path = files[i]
        object_name = file_path.split('/')[-1]
        response = s3_client.upload_file(file_path, bucket_name, object_name)
        print(f'File {object_name} uploaded successfully to bucket: {bucket_name}')
except NoCredentialsError:
    print('Credentials are not available or incorrect!')




