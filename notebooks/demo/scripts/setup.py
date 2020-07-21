#!/usr/bin/python
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

from setuptools import setup, find_packages

REQUIRED_PACKAGES = ['Keras==2.0.4','matplotlib==2.2.4','seaborn==0.9.0']

setup(
    name='trainer',
    version='1.0',
    packages=find_packages(),
    install_requires=REQUIRED_PACKAGES,
    author='Grid Dynamics ML Engineer',
    author_email='griddynamics@griddynamics.com',
    url='https://griddynamics.com'
)
