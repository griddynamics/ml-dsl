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

from com.griddynamics.dsl.ml.settings.arguments import Arguments
from com.griddynamics.dsl.ml.jobs.pyspark_job import PySparkJob


class JobBuilder:
    """Job builder implementation for jobs"""
    def files_root(self, files_root: str):
        self.__job.files_root = files_root
        return self

    def task_script(self, name: str, py_scripts: dict):
        script = py_scripts[name]
        if script is None:
            raise ValueError(f"There is no defined script with name: {name}")
        self.__job.task_script = script
        return self

    def arguments(self, arguments: Arguments):
        self.__job.args = arguments
        return self

    def py_script(self, name: str, py_scripts: dict):
        script = py_scripts[name]

        if script is None:
            raise ValueError(f"There is no defined script with name: {name}")

        self.__job.py_scripts = script
        return self

    def __init__(self, platform):
        self.reset(platform)

    def reset(self, platform):
        self.__job = PySparkJob(platform=platform)

    def build_job(self, profile, platform):
        if not (self.__job.job_file or self.__job.task_script):
            raise ValueError("Script for execution wasn't set")
        job = self.__job.from_profile(profile)
        self.reset(platform)
        return job

    def job_file(self, val):
        self.__job.job_file = val
        return self

    def run_async(self):
        self.__job.run_async = True
        return self

    def job_id(self, val):
        self.__job.job_id = val
        return self

    def py_file(self, val):
        self.__job.py_files = val
        return self

    def file(self, val):
        self.__job.files = val
        return self

    def max_failures(self, val):
        self.__job.max_failures = val
        return self

    def logging(self, val):
        self.__job.logging = val
        return self

    def jar(self, val):
        self.__job.jars = val
        return self

    def packages(self, val):
        self.__job.packages = val
        return self

    def property(self, key, val):
        self.__job.properties = {key: val}
        return self

    def archive(self, val):
        self.__job.archives = val
        return self

    def label(self, key, val):
        self.__job.labels = key, val
        return self

