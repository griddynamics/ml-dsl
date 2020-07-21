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
from json import JSONEncoder
from com.griddynamics.dsl.ml.py_script import PyScript
from com.griddynamics.dsl.ml.settings.arguments import Arguments
from com.griddynamics.dsl.ml.settings.description import Platform
from com.griddynamics.dsl.ml.helpers import Helper, Path


class PySparkJob(JSONEncoder):
    """PySpark job model
    """

    def __init__(self, py_spark_job=None, platform=Platform.GCP):
        super().__init__()
        if py_spark_job:
            self.__job_file = py_spark_job.main_python_file_uri
            self.__py_files = py_spark_job.python_file_uris
            self.__files = py_spark_job.file_uris
            self.__args = py_spark_job.args
            self.__properties = py_spark_job.properties
            self.__jars = py_spark_job.jar_file_uris
            self.__archives = py_spark_job.archive_uris
            self.__logging = py_spark_job.logging_config
            self.__platform = platform

        else:
            self.__job_file: Path = None
            self.__py_files = []
            self.__files = []
            self.__args = []
            self.__properties = {}
            self.__jars = []
            self.__packages = []
            self.__archives = []
            self.__logging = None
            self.__files_root = Path('')
            self.__task_script = None
            self.__run_async = False
            self.__job_id = None
            self.__py_scripts = []
            self.__max_failures = 0
            self.__labels = {}
            self.__platform = platform

    @property
    def labels(self):
        return self.__labels

    @property
    def files_root(self):
        return self.__files_root

    @property
    def job_file(self):
        return self.__job_file

    @property
    def task_script(self) -> PyScript:
        return self.__task_script

    @property
    def run_async(self):
        return self.__run_async

    @property
    def job_id(self):
        return self.__job_id

    @property
    def py_files(self):
        return self.__py_files

    @property
    def py_scripts(self):
        return self.__py_scripts

    @property
    def files(self):
        return self.__files

    @property
    def max_failures(self):
        return self.__max_failures

    @property
    def args(self):
        return self.__args

    @property
    def jars(self):
        return self.__jars

    @property
    def packages(self):
        return self.__packages

    @property
    def archives(self):
        return self.__archives

    @property
    def logging(self):
        return self.__logging

    @property
    def properties(self):
        return self.__properties

    @labels.setter
    def labels(self, label):
        key, value = label
        self.__labels[key] = value

    @files_root.setter
    def files_root(self, files_root: str):
        self.__files_root = Path(files_root)

    @job_file.setter
    def job_file(self, val: str, base_path=None):
        if base_path is None:
            base_path = self.files_root
        job_file = Helper.construct_path(val, base_path, self.__platform)
        self.__job_file = job_file

    @run_async.setter
    def run_async(self, val):
        self.__run_async = val

    @task_script.setter
    def task_script(self, task_script: PyScript):
        self.__task_script = task_script
        self.__py_scripts.append(task_script)

    @job_id.setter
    def job_id(self, val):
        self.__job_id = val

    @logging.setter
    def logging(self, val):
        self.__logging = val

    @max_failures.setter
    def max_failures(self, val):
        self.__max_failures = val

    @py_files.setter
    def py_files(self, val: str, base_path=None):
        if base_path is None:
            base_path = self.files_root
        py_file = Helper.construct_path(val, base_path, self.__platform)
        self.__py_files.append(py_file)

    @py_scripts.setter
    def py_scripts(self, val: PyScript):
        self.__py_scripts.append(val)

    @files.setter
    def files(self, val: str, base_path=None):
        if base_path is None:
            base_path = self.files_root
        file = Helper.construct_path(val, base_path, self.__platform)
        self.__files.append(file)

    @properties.setter
    def properties(self, val):
        self.__properties.update(val)

    @archives.setter
    def archives(self, val: str, base_path=None):
        if base_path is None:
            base_path = self.files_root
        arch = Helper.construct_path(val, base_path, self.__platform)
        self.__archives.append(arch)

    @packages.setter
    def packages(self, val: list):
        self.__packages = val

    @jars.setter
    def jars(self, val: str, base_path=None):
        if base_path is None:
            base_path = self.files_root
        jar = Helper.construct_path(val, base_path, self.__platform)
        self.__jars.append(jar)

    @args.setter
    def args(self, val: Arguments):
        self.__args += val.get()

    @staticmethod
    def generate_run_script(py_script: PyScript) -> PyScript:
        module = "".join(py_script.name.split(".")[:-1])
        module_part = py_script.name.split(".")[0] if module is None else module
        run_script = """from {} import {}\nif __name__ == '__main__':\n\t{}.run()""".format(module_part,
                                                                                            py_script.class_name,
                                                                                            py_script.class_name)
        return PyScript(run_script, 'run.py')

    def from_profile(self, profile):
        if profile.py_files:
            for pf in profile.py_files:
                self.py_files = pf
        if profile.files:
            for f in profile.files:
                self.files = f
        if profile.jars:
            for j in profile.jars:
                self.jars = j
        if profile.properties:
            self.properties = profile.properties
        if profile.archives:
            for arch in profile.archives:
                self.archives = arch
        if profile.logging:
            self.logging = profile.logging
        if profile.max_failures:
            self.max_failures = profile.max_failures
        if profile.packages:
            self.packages = profile.packages
        if profile.args:
            arguments = Arguments()
            arguments.set_args(**profile.args)
            self.args = arguments
        return self

    def from_json(self, json):
        self.__job_id = json['job_id']
        self.__job_file = json.get('main_python_file_uri')
        if json.get('args'):
            self.__args = json.get('args')
        if json.get('archive_uris'):
            self.__archives = json.get('archive_uris')
        if json.get('jar_file_uris'):
            self.__jars = json.get('jar_file_uris')
        if json.get('file_uris'):
            self.__files = json.get('file_uris')
        if json.get('python_file_uris'):
            self.__py_files = json.get('python_file_uris')
        if json.get('logging_config'):
            self.__logging = json.get('logging_config')
        if json.get('properties'):
            self.__properties = json.get('properties')
