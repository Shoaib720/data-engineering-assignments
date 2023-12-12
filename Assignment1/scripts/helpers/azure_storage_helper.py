from azure.storage.blob import BlobServiceClient

def upload_file_to_azure_blob(connection_string,container_name,file_path):
  blob_service_client = BlobServiceClient.from_connection_string(connection_string)
  blob_client = blob_service_client.get_blob_client(container_name,blob=str(file_path).split('/')[-1])
  with open(file_path,'rb') as data:
    blob_client.upload_blob(data,overwrite=True)
  print(f'Uploaded {str(file_path).split("/")[-1]} to blob')