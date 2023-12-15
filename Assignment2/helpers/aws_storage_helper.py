from boto3 import client,Session

def get_session(access_key_id,secret_access_key):
    botosession = Session(
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key
    )

    return botosession

def download_file_from_s3(bucket, key,localfilepath, session):
    print(f'Downloading {key}......')
    s3 = session.client('s3')
    s3.download_file(
        Bucket=bucket, Key=key, Filename=localfilepath
    )
    print(f'File {key} downloaded at {localfilepath} successfully')