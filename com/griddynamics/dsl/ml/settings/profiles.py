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

import json


class Profile(object):
    _profiles = {}

    def __init__(self, root_path, bucket, project, cluster, region, ai_region, job_prefix, job_async):
        self.root_path = root_path
        self.bucket = bucket
        self.project = project
        self.cluster = cluster
        self.region = region
        self.ai_region = ai_region
        self.job_prefix = job_prefix
        self.job_async = job_async

    @staticmethod
    def set(name, profile):
        Profile._profiles[name] = profile

    @staticmethod
    def get(name):
        profile = Profile._profiles.get(name, None)
        if profile is None:
            if name == 'DemoProfile':
                profile = Profile(root_path='/home/jovyan/work/data/demo/scripts',
                                  bucket='ai4ops',
                                  project='gd-gcp-techlead-experiments',
                                  cluster='ai4ops',
                                  region='global',
                                  ai_region='us-central1',
                                  job_prefix='demo_job',
                                  job_async=False)
                Profile._profiles[name] = profile
        return profile

    @staticmethod
    def load_from_file(file_path):
        with open(file_path, 'r') as fp:
            field = json.load(fp)
        return field


class AIProfile(Profile):
    _profiles = {}

    def __init__(self, root_path, bucket, project, cluster, region, ai_region, job_prefix, job_async,
                 package_dst, scale_tier, package_name, runtime_version):
        super(AIProfile, self).__init__(root_path, bucket, project, cluster, region, ai_region, job_prefix, job_async)
        self.package_name = package_name
        self.package_dst = package_dst
        self.scale_tier = scale_tier
        self.runtime_version = runtime_version

    @staticmethod
    def get(name):
        profile = Profile._profiles.get(name, None)
        if profile is None:
            if name == 'DemoAIProfile':
                profile = AIProfile(root_path='/home/jovyan/work/data/demo/scripts',
                                    bucket='ai4ops',
                                    project='gd-gcp-techlead-experiments',
                                    cluster='ai4ops',
                                    region='global',
                                    ai_region='us-central1',
                                    job_prefix='ai_train_demo_job',
                                    job_async=False,
                                    package_name='trainer',
                                    package_dst='mldsl/packages',
                                    scale_tier='BASIC',
                                    runtime_version='1.14')
                AIProfile._profiles[name] = profile
            if name == 'DemoDeployAIProfile':
                profile = AIProfile(root_path='/home/jovyan/work/data/demo/deploy',
                                    bucket='ai4ops',
                                    project='gd-gcp-techlead-experiments',
                                    cluster='ai4ops',
                                    region='global',
                                    ai_region='us-central1',
                                    job_prefix='ai_train_demo_job',
                                    job_async=False,
                                    package_name='',
                                    package_dst='staging',
                                    scale_tier='BASIC',
                                    runtime_version='1.14')
                AIProfile._profiles[name] = profile
        return profile

    @staticmethod
    def set(name, profile):
        Profile._profiles[name] = profile


class PySparkJobProfile(Profile):
    _profiles = {}

    def __init__(self, root_path, bucket, project, cluster, region, ai_region, job_prefix, job_async):
        super(PySparkJobProfile, self).__init__(root_path, bucket, project, cluster, region, ai_region, job_prefix,
                                                job_async)
        # List of files associated with the job
        self.py_files = []
        self.files = []
        self.jars = []
        # str dict of job properties
        # Example: {"spark.executor.cores":"1","spark.executor.memory":"4G"}
        self.properties = {}
        # str Args of the job
        # Example {"table": "my_table", "duration": "100"}
        self.args = {}
        self.archives = []
        self.packages = []
        # The runtime logging config of the job.
        # Optional. The runtime log config for job execution.
        self.logging = {
            # The per-package log levels for the driver.
            # This may include "root" package name to configure rootLogger.
            # Examples:  'com.google = 4', 'root = 4', 'org.apache = 1'
            "driver_log_levels": {},
        }
        self.max_failures = 0

    @staticmethod
    def get(name):
        profile = Profile._profiles.get(name, None)
        if profile is None:
            raise ValueError("There is no specified profile")
        return profile

    @staticmethod
    def set(name, profile):
        Profile._profiles[name] = profile
