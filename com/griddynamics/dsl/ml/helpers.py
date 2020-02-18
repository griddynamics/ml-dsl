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
    def delete_path_from_storage(bucket_name, path):
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=path)
        for blob in blobs:
            blob.delete()

    @staticmethod
    def copy_folder_on_storage(bucket_name, path_from: str, path_to: str):
        path_from = path_from.replace(f'gs://{bucket_name}/', "")
        path_to = path_to.replace(f'gs://{bucket_name}/', "")

        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=path_from)
        for blob in blobs:
            bucket.copy_blob(blob, bucket, path_to + blob.name.replace(path_from, path_to))

    @staticmethod
    def download_folder_from_storage(bucket_name, path_from, path_to):
        path_from = path_from.replace(f'gs://{bucket_name}/', "")
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=path_from, delimiter="/")

        for blob in blobs:
            destination_uri = f'{path_to} / {blob.name}'
            blob.download_to_filename(destination_uri)

    @staticmethod
    def upload_file_to_storage(project_id, bucket, file_path: str, gs_path):
        gs_path = gs_path.replace(f'gs://{bucket}/', "")
        client = storage.Client(project=project_id)
        bucket = client.get_bucket(bucket)
        blob = bucket.blob('{}/{}'.format(gs_path, file_path.split('/')[-1]))
        blob.upload_from_filename(file_path)

    @staticmethod
    def copy_file_on_storage(bucket_name, blob_name, new_blob_name, new_bucket_name=None):
        """Copies a blob from one bucket to another with a new name."""
        if new_bucket_name is None:
            new_bucket_name = bucket_name

        storage_client = storage.Client()
        source_bucket = storage_client.get_bucket(bucket_name)
        source_blob = source_bucket.blob(blob_name.replace(f'gs://{bucket_name}/', ""))
        destination_bucket = storage_client.get_bucket(new_bucket_name)

        source_bucket.copy_blob(
            source_blob, destination_bucket, new_blob_name.replace(f'gs://{new_bucket_name}/', ""))

    @staticmethod
    def construct_path(filename: str, base_path):
        return filename if filename.startswith('gs://') else Path(base_path) / filename
