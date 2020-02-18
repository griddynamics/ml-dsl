#!/usr/bin/python
from google.cloud import storage

def get_bucket(bucket_id, project):
    client = storage.Client(project)
    return client.get_bucket(bucket_id)


def upload_to_gs(bct, folder_path_gs, file):
    blob = bct.blob(folder_path_gs + '/' + file)
    blob.upload_from_filename(file)

def print_text(text):
    print(text)
    
print_text(get_bucket(BUCKET, PROJECT).__dict__) 
