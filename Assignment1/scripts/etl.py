from datetime import datetime, timedelta
import pandas as pd
import os
from helpers.aws_storage_helper import get_session, download_file_from_s3
from helpers.azure_storage_helper import upload_file_to_azure_blob
import logging

default_args = {
  'owner': 'airflow',
  'retries': 2,
  'retry_delay': timedelta(minutes=1)
}

AWS_ACCESS_KEY_ID = ''
AWS_SECRET_ACCESS_KEY = ''
AWS_BUCKET_NAME = ''
AZURE_CONTAINER_CONNECTION_STRING = ''
AZURE_RAW_DATA_CONTAINER_NAME = ''

def download_excel_file(key):
  botosession = get_session(
    access_key_id=AWS_ACCESS_KEY_ID,
    secret_access_key=AWS_SECRET_ACCESS_KEY
  )

  download_file_from_s3(
    bucket=AWS_BUCKET_NAME,
    key=key,
    localfilepath=str(key).split('/')[-1],
    session=botosession
  )
  logging.info(f'Downloaded {key} to {str(key).split("/")[-1]}')

def convert_excel_to_df(excel_file_path):
  with open(excel_file_path,'rb') as file:
    return pd.read_excel(file)

def merge_dataframes(dframe1, dframe1_cols, dframe2, dframe2_cols, key, join_type):
  return dframe1[dframe1_cols].merge(
    dframe2[dframe2_cols],
    on = key,
    how = join_type
  )

def merge_excels():
  excel1_df = convert_excel_to_df(os.path.join('Excel_1.xlsx'))
  excel2_df = convert_excel_to_df(os.path.join('Excel_2.xlsx'))
  excel3_df = convert_excel_to_df(os.path.join('Excel_3.xlsx'))
  merged_e1_e2 = merge_dataframes(
    dframe1=excel1_df,
    dframe1_cols=['id','fname','lname','date_of_birth','email','gender','age','contact','city'],
    dframe2=excel2_df,
    dframe2_cols=['id','address','country'],
    key='id',
    join_type='left'
  )
  merged = merge_dataframes(
    dframe1=merged_e1_e2,
    dframe1_cols=['id','fname','lname','date_of_birth','email','gender','age','contact','city','address','country'],
    dframe2=excel3_df,
    dframe2_cols=['id'],
    key='id',
    join_type='inner'
  )
  merged.to_excel(os.path.join('merged_excels.xlsx'))
  logging.info(f'Merged the excels into {os.path.join("merged_excels.xlsx")}')
  upload_file_to_azure_blob(
    connection_string=AZURE_CONTAINER_CONNECTION_STRING,
    container_name=AZURE_RAW_DATA_CONTAINER_NAME,
    file_path=os.path.join('merged_excels.xlsx')
  )
  logging.info(f'Uploaded file {os.path.join("merged_excels.xlsx")} to azure blob')

download_excel_file('dummy-data/Excel_1.xlsx')
download_excel_file('dummy-data/Excel_2.xlsx')
download_excel_file('dummy-data/Excel_3.xlsx')
merge_excels()