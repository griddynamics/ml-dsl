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


from abc import ABC, abstractmethod
from pathlib import Path

# noinspection PyUnresolvedReferences
from google.cloud import dataproc_v1
from google.auth import compute_engine
# noinspection PyUnresolvedReferences
from google.cloud.dataproc_v1.gapic.transports import job_controller_grpc_transport
from googleapiclient import discovery

import boto3
import sagemaker

from com.griddynamics.dsl.ml.settings.description import Platform


class CompositeSession:
    __ml_session = None
    __job_session = None

    __local_project_root = Path("")
    __env_project_root = None

    @abstractmethod
    def get_job_session(self):
        return self.__job_session

    @abstractmethod
    def get_ml_session(self):
        return self.__ml_session

    def __build(self, job_bucket, job_region, cluster, job_project_id, platform, job_path='jobs-root',
                ml_region=None, ml_project_id=None, ml_bucket=None, use_cloud_engine_credentials=False):
        self.__job_session = Session(job_bucket,
                                     job_region,
                                     cluster,
                                     job_project_id,
                                     platform,
                                     job_path,
                                     use_cloud_engine_credentials=use_cloud_engine_credentials)
        if ml_project_id is None:
            ml_project_id = job_project_id
        if ml_region is None:
            ml_region = job_region
        if ml_bucket is None:
            ml_bucket = job_bucket        
        self.__ml_session = Session(ml_bucket, ml_region, None, ml_project_id, platform, None,
                                    use_cloud_engine_credentials=use_cloud_engine_credentials)

    def __init__(self, job_bucket, job_region, cluster, job_project_id, job_path, ml_region,
                 ml_project_id, ml_bucket, platform, use_cloud_engine_credentials=False):
        self.use_cloud_engine_credentials = use_cloud_engine_credentials
        if self.__ml_session is None and self.__job_session is None:
            self.__build(job_bucket, job_region, cluster, job_project_id, platform, job_path, ml_region,
                         ml_project_id, ml_bucket, use_cloud_engine_credentials=use_cloud_engine_credentials)


class Session:
    """Session of current run"""
    def __init__(self, bucket, zone, cluster, project_id, platform, job_path='jobs-root',
                 use_cloud_engine_credentials=False):
        self.__bucket = bucket
        self.__jobs_path = job_path
        self.__zone = zone
        self.__cluster = cluster
        self.__project_id = project_id
        self.__region = None
        self.__cluster_uuid = None
        self.__platform = platform

        if self.__platform == 'GCP':
            if self.__zone == 'global':
                self.__region = self.__zone
            else:
                self.__region = self.get_region_from_zone(self.__zone)

            credentials = None
            if use_cloud_engine_credentials:
                credentials = compute_engine.Credentials()
            if cluster is None and job_path is None:
                self._cloudml = discovery.build('ml', 'v1', credentials=credentials)
            else:
                if self.zone == 'global':
                    self._dataproc_job_client = dataproc_v1.JobControllerClient(credentials=credentials)
                else:
                    job_transport = (
                        job_controller_grpc_transport.JobControllerGrpcTransport(
                            address='{}-dataproc.googleapis.com:443'.format(self.__region),
                            credentials=credentials))
                    self._dataproc_job_client = dataproc_v1.JobControllerClient(job_transport)
        else:
                self._session = boto3.Session()
                self._sm_session = sagemaker.Session()
                self._role = sagemaker.get_execution_role()

    @staticmethod
    def get_region_from_zone(zone):
        try:
            region_as_list = zone.split('-')[:-1]
            if zone.count("-") > 1 and len(region_as_list[-1]) == 1:
                return '-'.join(region_as_list)
            else:
                zone
        except (AttributeError, IndexError, ValueError):
            raise ValueError('Invalid zone provided, please check your input.')

    @property
    def zone(self):
        return self.__zone

    @property
    def bucket(self):
        return self.__bucket

    @property
    def project_id(self):
        return self.__project_id

    @property
    def cluster(self):
        return self.__cluster

    @property
    def jobs_path(self):
        return self.__jobs_path

    @property
    def cluster_uuid(self):
        return self.__cluster_uuid

    @property
    def region(self):
        return self.__region

    @staticmethod
    def load_from_file(bucket_name, filename):
        pass

    @zone.setter
    def zone(self, zone):
        self.__zone = zone


class AbstractSessionFactory(ABC):
    @abstractmethod
    def build_session(self, **kwargs):
        pass

    @staticmethod
    def get_session():
        pass


class SessionFactory:
    def __new__(cls, platform: Platform):
        if platform is Platform.GCP:
            return GCPSessionFactory()
        elif platform is Platform.AWS:
            return AWSSessionFactory()
        else:
            ValueError("Unsupported Platform")


class GCPSessionFactory(AbstractSessionFactory):
    __composite_session = None

    def get_session(self):
        return self.__composite_session

    @staticmethod
    def build_session(job_bucket, job_region, cluster, job_project_id, ml_region=None, project_local_root='jobs-root',
                      project_env_root='', ml_project_id=None, ml_bucket=None, use_cloud_engine_credentials=False):
        if GCPSessionFactory.__composite_session is None:
            GCPSessionFactory.__composite_session = CompositeSession(job_bucket, job_region, cluster, job_project_id,
                                                                     project_local_root,
                                                                     ml_region,
                                                                     ml_project_id, ml_bucket,
                                                                     use_cloud_engine_credentials=use_cloud_engine_credentials,
                                                                     platform='GCP')
        return GCPSessionFactory.__composite_session


class AWSSessionFactory(AbstractSessionFactory):
    __composite_session = None

    def get_session(self):
        return self.__composite_session

    @staticmethod
    def build_session(job_bucket, job_region, cluster, job_project_id, ml_region=None, project_local_root='jobs-root',
                      project_env_root='', ml_project_id=None, ml_bucket=None, use_cloud_engine_credentials=False,
                      platform='AWS'):
        if AWSSessionFactory.__composite_session is None:
            AWSSessionFactory.__composite_session = CompositeSession(job_bucket, job_region, cluster, job_project_id,
                                                                     project_local_root,
                                                                     ml_region,
                                                                     ml_project_id, ml_bucket,
                                                                     use_cloud_engine_credentials=use_cloud_engine_credentials,
                                                                     platform='AWS')
        return AWSSessionFactory.__composite_session
