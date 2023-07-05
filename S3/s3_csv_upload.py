import os
import boto3
import datetime
# import pandas as pd
# import awswrangler as wr
from dotenv import load_dotenv

# Loading environment variables from .env file
load_dotenv()

# Getting Access Variables from .env file 
aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')

# Defining the bucket's name and results path
bucket_name = os.environ.get('BUCKET_NAME')
results_dir = os.environ.get('KEY_PROCESSED')

def load_csv_to_s3(bucket_name):

    # Setting AWS Session using Access Variables
    s3 = boto3.resource('s3',
                        aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key)
 
    # The inserted date will act as a path inside of the bucket
    day_dir = input('Enter desired date as the example: YYYY/MM/DD') 
    dt_day= datetime.datetime.strptime(day_dir, format)
    df = "insert data here"
    # Saves the files in a S3 bucket
    # time.sleep(3)
    s3.Object(f"{bucket_name}/{results_dir}/{day_dir}", f"daily_agg_{dt_day}_EF.csv").put(Body=df)

    print("Csv loaded!")

