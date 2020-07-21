# Copyright (c) 2020 Grid Dynamics International, Inc. All Rights Reserved
# http://www.griddynamics.com
# Classification level: PUBLIC
# Licensed under the Apache License, Version 2.0(the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# Id:          ML-DSL
# Project:     ML DSL
# Description: DSL to configure and execute ML/DS pipelines

import setuptools

setuptools.setup(
    name='griddynamics-dsl-ml',
    version='1.0.0',
    description='Module for building ML pipelines',
    author='Grid Dynamics Inc.',
    url='https://github.com/griddynamics',
    author_email='arodin@griddynamics.com',
    packages=["com.griddynamics.dsl.ml", "com.griddynamics.dsl.ml.settings", "com.griddynamics.dsl.ml.models",
              "com.griddynamics.dsl.ml.jobs", "com.griddynamics.dsl.ml.executors"],
    install_requires=['retrying>=1.3.3',
                      'pyspark', 'google-cloud-dataproc', 'google-cloud-logging', 'sagemaker', 'boto3'],
)
