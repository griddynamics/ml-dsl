# Copyright (c) 2020 Grid Dynamics International, Inc. All Rights Reserved
# http://www.griddynamics.com
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# Id:          ML_PLATFORM
# Project:     ML Platform
# Description: DSL to configure and execute ML/DS pipelines

import distutils
import importlib
import os
from pathlib import Path
from unittest import mock

import setuptools
from google.cloud import storage
from google.auth import compute_engine

import boto3
from botocore.exceptions import ClientError


class Helper:

    @staticmethod
    def build_package(name, script_names: [], script_args=None, version="1.0", requires=None):
        distutils.core.setup(scripts=script_names,
                             name=name,
                             version=version,
                             requires=requires,
                             script_args=script_args)

    @staticmethod
    def get_file(file):
        if file:
            f = open(file, "rb")
            return f, Helper.get_file_name(file)

    @staticmethod
    def get_file_name(path):
        return os.path.basename(path)

    @staticmethod
    def get_setup_params(setup_path):
        spec = importlib.util.spec_from_file_location("setup", setup_path)
        setup = importlib.util.module_from_spec(spec)

        with mock.patch.object(setuptools, 'setup') as mock_setup:
            spec.loader.exec_module(setup)  # This is setup.py which calls setuptools.setup

        # called arguments are in `mock_setup.call_args`
        args, kwargs = mock_setup.call_args
        return kwargs

    @staticmethod
    def build_package_name_from_params(kwargs, extension):
        return f"{kwargs['name']}-{kwargs['version']}.{extension}"


class GCPHelper(Helper):

    @staticmethod
    def delete_path_from_storage(bucket_name, path, use_cloud_engine_credentials=False):
        credentials = None
        if use_cloud_engine_credentials:
            credentials = compute_engine.Credentials()

        storage_client = storage.Client(credentials=credentials)
        bucket = storage_client.get_bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=path)
        for blob in blobs:
            blob.delete()

    @staticmethod
    def copy_folder_on_storage(bucket_name, path_from: str, path_to: str, use_cloud_engine_credentials=False):
        path_from = path_from.replace(f'gs://{bucket_name}/', "")
        path_to = path_to.replace(f'gs://{bucket_name}/', "")
        credentials = None
        if use_cloud_engine_credentials:
            credentials = compute_engine.Credentials()

        storage_client = storage.Client(credentials=credentials)
        bucket = storage_client.get_bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=path_from)
        for blob in blobs:
            bucket.copy_blob(blob, bucket, path_to + blob.name.replace(path_from, path_to))

    @staticmethod
    def download_folder_from_storage(bucket_name, path_from, path_to, use_cloud_engine_credentials=False):
        path_from = path_from.replace(f'gs://{bucket_name}/', "")
        credentials = None
        if use_cloud_engine_credentials:
            credentials = compute_engine.Credentials()
        storage_client = storage.Client(credentials=credentials)
        bucket = storage_client.get_bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=path_from, delimiter="/")

        for blob in blobs:
            destination_uri = f'{path_to} / {blob.name}'
            blob.download_to_filename(destination_uri)

    @staticmethod
    def upload_file_to_storage(project_id, bucket, file_path: str, gs_path, use_cloud_engine_credentials=False):
        gs_path = gs_path.replace(f'gs://{bucket}/', "")
        credentials = None
        if use_cloud_engine_credentials:
            credentials = compute_engine.Credentials()
        client = storage.Client(project=project_id, credentials=credentials)
        bucket = client.get_bucket(bucket)
        blob = bucket.blob('{}/{}'.format(gs_path, file_path.split('/')[-1]))
        blob.upload_from_filename(file_path)

    @staticmethod
    def copy_file_on_storage(bucket_name, blob_name, new_blob_name, new_bucket_name=None,
                             use_cloud_engine_credentials=False):
        """Copies a blob from one bucket to another with a new name."""
        if new_bucket_name is None:
            new_bucket_name = bucket_name

        credentials = None
        if use_cloud_engine_credentials:
            credentials = compute_engine.Credentials()
        storage_client = storage.Client(credentials=credentials)
        source_bucket = storage_client.get_bucket(bucket_name)
        source_blob = source_bucket.blob(blob_name.replace(f'gs://{bucket_name}/', ""))
        destination_bucket = storage_client.get_bucket(new_bucket_name)

        source_bucket.copy_blob(
            source_blob, destination_bucket, new_blob_name.replace(f'gs://{new_bucket_name}/', ""))

    @staticmethod
    def construct_path(filename: str, base_path):
        return filename if filename.startswith('gs://') else Path(base_path) / filename


class AWSHelper(Helper):

    @staticmethod
    def delete_path_from_storage(bucket_name, path):
        s3 = boto3.client('s3')
        # Delete the object
        try:
            s3.delete_object(Bucket=bucket_name, Key=path)
        except ClientError as e:
            raise e

    @staticmethod
    def copy_folder_on_storage(bucket_name, path_from: str, path_to: str):
        s3 = boto3.client("s3")
        objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=path_from)['Contents']
        for obj in objects:
            dest = obj['Key'].replace(path_from, path_to)
            copy_source = {'Bucket': bucket_name, 'Key': obj['Key']}
            s3.copy_object(CopySource=copy_source, Bucket=bucket_name, Key=dest)

    @staticmethod
    def download_folder_from_storage(bucket_name, path_from, path_to):
        s3 = boto3.client("s3")
        objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=path_from)['Contents']
        for obj in objects:
            dest = obj['Key'].replace(path_from, path_to)
            if not os.path.exists(os.path.dirname(dest)):
                os.makedirs(os.path.dirname(dest))
            try:
                s3.download_file(bucket_name, obj['Key'], dest)
            except IsADirectoryError:
                pass

    @staticmethod
    def upload_file_to_storage(bucket, file_name, object_name):
        if object_name is None:
            object_name = file_name
        # Upload the file
        s3_client = boto3.client('s3')
        try:
            response = s3_client.upload_file(file_name, bucket, object_name)
        except ClientError as e:
            raise e

    @staticmethod
    def copy_object(src_bucket_name, src_object_name, dest_bucket_name=None, dest_object_name=None):
        copy_source = {'Bucket': src_bucket_name, 'Key': src_object_name}
        if dest_object_name is None:
            dest_object_name = src_object_name
        if dest_bucket_name is None:
            dest_bucket_name = src_bucket_name
        # Copy the object
        s3 = boto3.client('s3')
        try:
            s3.copy_object(CopySource=copy_source, Bucket=dest_bucket_name, Key=dest_object_name)
        except ClientError as e:
            raise e
