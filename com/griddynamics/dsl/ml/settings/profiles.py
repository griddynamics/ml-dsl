# Copyright (c) 2020 Grid Dynamics International, Inc. All Rights Reserved
# http://www.griddynamics.com
# Classification level: PUBLIC
# Licensed under the Apache License, Version 2.0(the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE - 2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# Id:          ML_PLATFORM
# Project:     ML Platform
# Description: DSL to configure and execute ML/DS pipelines

import json


class BaseProfile(object):
    _profiles = {}

    def __init__(self, bucket, cluster, region, job_prefix):
        self.bucket = bucket
        self.cluster = cluster
        self.region = region
        self.job_prefix = job_prefix

    @staticmethod
    def set(name, profile):
        BaseProfile._profiles[name] = profile

    @staticmethod
    def get(name):
        profile = BaseProfile._profiles.get(name, None)
        return profile

    @staticmethod
    def load_profile_data(file_path):
        with open(file_path, 'r') as fp:
            field = json.load(fp)
        return field


class Profile(BaseProfile):

    def __init__(self, bucket, cluster, region, job_prefix, root_path, project, ai_region, job_async):
        super(Profile, self).__init__(bucket, cluster, region, job_prefix)
        self.root_path = root_path
        self.project = project
        self.ai_region = ai_region
        self.job_async = job_async


class PySparkJobProfile(Profile):

    def __init__(self, bucket, cluster, region, job_prefix, root_path, project, ai_region, job_async,
                 use_cloud_engine_credentials=False):
        super(PySparkJobProfile, self).__init__(bucket, cluster, region, job_prefix, root_path,
                                                project, ai_region, job_async)
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
        self.use_cloud_engine_credentials = use_cloud_engine_credentials


class AIProfile(Profile):

    def __init__(self, bucket, cluster, region, job_prefix, root_path, project, ai_region, job_async,
                 runtime_version, python_version,
                 package_dst='mldsl/packages', scale_tier='BASIC', package_name='mldsl',
                 use_cloud_engine_credentials=False, arguments={}):
        super(AIProfile, self).__init__(bucket, cluster, region, job_prefix,
                                        root_path, project, ai_region, job_async)

        self.package_name = package_name
        self.package_dst = package_dst
        self.scale_tier = scale_tier
        self.runtime_version = runtime_version
        self.python_version  = python_version
        self.use_cloud_engine_credentials = use_cloud_engine_credentials
        self.arguments = arguments


class DeployAIProfile(AIProfile):

    def __init__(self, bucket, cluster, region, job_prefix,
                 root_path, project, ai_region, job_async,
                 runtime_version, python_version,
                 package_dst=None, scale_tier='BASIC', package_name=None,
                 use_cloud_engine_credentials=False, arguments={},
                 version_name='v1', is_new_model='True',
                 artifacts=[], custom_code=None, path_to_saved_model='./'):
        super(DeployAIProfile, self).__init__(bucket, cluster, region, job_prefix,
                                              root_path, project, ai_region, job_async,
                                              runtime_version, python_version,
                                              package_dst, scale_tier, package_name,
                                              use_cloud_engine_credentials, arguments)
        self.version_name = version_name
        self.is_new_model = is_new_model
        self.artifacts = artifacts
        self.custom_code = custom_code
        self.path_to_saved_model = path_to_saved_model


class SageMakerProfile(BaseProfile):
    _profiles = {}

    def __init__(self, bucket, cluster, region, job_prefix, container,
                 root_path=None, framework_version=None,
                 instance_type="ml.t2.medium", instance_count=1,
                 use_cloud_engine_credentials=False):
        super(SageMakerProfile, self).__init__(bucket, cluster, region, job_prefix)
        self.container = container
        self.root_path = root_path
        self.framework_version = framework_version
        self.instance_type = instance_type
        self.instance_count = instance_count
        self.endpoint_name = None
        self.model_data = None
        self.py_version = None
        self.use_cloud_engine_credentials = use_cloud_engine_credentials

