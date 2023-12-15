import requests
import json
import os
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from helpers.aws_storage_helper import get_session, download_file_from_s3
from helpers.postgres_helper import postgres2csv
from helpers.mail_helper import send_mail
import pandas as pd

# definitions
AWS_ACCESS_KEY_ID = ''
AWS_SECRET_ACCESS_KEY = ''

spark = SparkSession.builder.master('local').appName("DvDRental").getOrCreate()

def create_date_subfolder(parent):
    path = os.path.join(parent,str(date.today()))
    if not (os.path.exists(path)):
        os.mkdir(path)
        print(f'Created folder {path}')
    else:
        print(f'Folder {path} already exists')
    return path

def convert_file_to_dataframe(filepath):
    df = None
    file_ext = str(filepath).split('/')[-1].split('.')[-1]
    if(file_ext == 'csv'):
        df = spark.read.csv(filepath,header=True)
    elif(file_ext == 'json'):
        df = spark.read.json(filepath)
    elif(file_ext == 'xlsx'):
        pd_dataframe = pd.read_excel(filepath)
        df = spark.createDataFrame(pd_dataframe)
    elif(file_ext == 'parquet'):
        df = spark.read.parquet(filepath)
    return df

# Extract

RAW_DATA_FOLDER = create_date_subfolder('raw_data')
TRANSFORMED_DATA_FOLDER = create_date_subfolder('transformed_data')

# extract from api
api_data = requests.get('https://raw.githubusercontent.com/Shoaib720/dvdrental-csv/main/film.json').json()
with open(f'{RAW_DATA_FOLDER}/film.json','w') as f:
    json.dump(api_data,f)
    
# extract from aws s3
aws_s3_source_file_keys = ['payment.xlsx','rental.parquet','staff.csv','store.csv']
awssession = get_session(access_key_id=AWS_ACCESS_KEY_ID,secret_access_key=AWS_SECRET_ACCESS_KEY)
for key in aws_s3_source_file_keys:
    download_file_from_s3(
        bucket='airflow-etl-hadiya',
        key=os.path.join('dummy-data',key),
        localfilepath=os.path.join(f'{RAW_DATA_FOLDER}',key),
        session=awssession
    )

# extract from postgres
tables = ['film_category','category','inventory']
postgres2csv(tables,f'{RAW_DATA_FOLDER}',{
    'host': '',
    'database': 'dvdrentaldb',
    'user': '',
    'password': ''
})

# Transform 
rentaldf = convert_file_to_dataframe(os.path.join(RAW_DATA_FOLDER,'rental.parquet'))
inventorydf = convert_file_to_dataframe(os.path.join(RAW_DATA_FOLDER,'inventory.csv'))
filmdf = convert_file_to_dataframe(os.path.join(RAW_DATA_FOLDER,'film.json'))
categorydf = convert_file_to_dataframe(os.path.join(RAW_DATA_FOLDER,'category.csv'))
filmcatdf = convert_file_to_dataframe(os.path.join(RAW_DATA_FOLDER,'film_category.csv'))
paymentdf = convert_file_to_dataframe(os.path.join(RAW_DATA_FOLDER,'payment.xlsx'))
staffdf = convert_file_to_dataframe(os.path.join(RAW_DATA_FOLDER,'staff.csv'))
storedf = convert_file_to_dataframe(os.path.join(RAW_DATA_FOLDER,'store.csv'))

print('Merging dataframes...')

dim_film_collection = filmdf.join(
    filmcatdf.drop('last_update'),
    'film_id',
    'inner'
).join(
    categorydf.select('category_id',col('name').alias('category')),
    'category_id',
    'inner'
).drop('category_id').collect()

dim_film_df = spark.createDataFrame(dim_film_collection)

fact_rental_collection = rentaldf.join(
    paymentdf.select('rental_id','amount'),
    'rental_id',
    'inner'
).join(
    inventorydf.drop('last_update'),
    'inventory_id',
    'inner'
).collect()

fact_rental_df = spark.createDataFrame(fact_rental_collection)

# Load

fact_rental_df.write.parquet(f'{TRANSFORMED_DATA_FOLDER}/fact_rental.parquet',mode='overwrite')
dim_film_df.write.parquet(f'{TRANSFORMED_DATA_FOLDER}/dim_film.parquet',mode='overwrite')

# Notify on success

message = f"""
DvDrental ETL Pipeline succeeded.
Transformed files stored at {TRANSFORMED_DATA_FOLDER}.

Regards,
Shoaib S. Shaikh
"""

send_mail(to='shoaib@gmail.com',subject='DvDRental ETL Pipeline Run',message=message)