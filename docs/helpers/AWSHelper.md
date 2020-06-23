### Class AWSHelper   
**class com.griddynamics.dsl.ml.helpers.AWSHelper**   
Class AWSHelper for organizing work with files, folders and buckets on Amazon S3.           
#### Methods   
> **delete_path_from_storage(bucket_name, path)**
> > Static method. Delete object from bucket.
>
> **copy_folder_on_storage(bucket_name, path_from: str, path_to: str)**
> > Static method. Copy folder on bucket.
>
> **download_folder_from_storage(bucket_name, path_from, path_to)**
> > Static method. Download folder with objects from bucket.
>
> **upload_file_to_storage(bucket, file_name, object_name)**
> > Static method. Upload file to AWS S3 with given object_name path.
>
> **copy_object(source_bucket, source_object_name, dest_bucket=None, dest_object_name=None)**
> > Static method. Copy file in/between bucket/buckets.
> 
> **upload_object_to_storage(obj, bucket, object_name)**
> > Static method. Upload file/folder to AWS S3 with given object_name path.






