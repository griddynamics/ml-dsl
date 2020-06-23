### Class GCPHelper   
**class com.griddynamics.dsl.ml.helpers.GCPHelper**   
Class GCPHelper for organizing work with files, folders and buckets on Google Cloud Storage.        
#### Methods   
> **delete_path_from_storage(bucket_name, path)**
> > Static method. Delete object from bucket.
>
> **copy_folder_on_storage(bucket_name, path_from: str, path_to: str)**
> > Static method. Copy folder on bucket.
>
> **download_folder_from_storage(bucket_name, path_from, path_to)**
> > Static method. Download folder with objects from Bucket.
>
> **upload_file_to_storage(project_id, bucket, file_path: str, gs_path)**
> > Static method. Upload file to GCS bucket.
>
> **copy_file_on_storage(bucket_name, blob_name, new_blob_name, new_bucket_name=None)**
> > Static method. Copy file in/between bucket/buckets.

