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

from com.griddynamics.dsl.ml.settings.artifacts import Artifact
from com.griddynamics.dsl.ml.settings.arguments import Arguments
from com.griddynamics.dsl.ml.helpers import Path


class Model:
    def __init__(self):
        self.__files_root = Path('')
        self.__name = None
        self.__version = None
        self.__model_file = None
        self.__model_uri = None
        self.__train_job_path = None
        self.__is_tuned = False
        self.__train_arguments = []
        self.__artifacts = []
        self.__custom_predictor_path = None
        self.__train_image_uri = None
        self.__train_module = None
        self.__packages = []
        self.__train_job_id = None

    @property
    def files_root(self):
        return self.__files_root

    @property
    def name(self):
        return self.__name

    @property
    def version(self):
        return self.__version

    @property
    def model_file(self):
        return self.__model_file

    @property
    def model_uri(self):
        return self.__model_uri

    @property
    def model_file(self):
        return self.__model_file

    @property
    def train_job_path(self):
        return self.__train_job_path

    @property
    def is_tuned(self):
        return self.__is_tuned

    @property
    def train_arguments(self):
        return self.__train_arguments

    @property
    def artifacts(self):
        return self.__artifacts

    @property
    def custom_predictor_path(self):
        return self.__custom_predictor_path

    @property
    def train_image_uri(self):
        return self.__train_image_uri

    @property
    def train_module(self):
        return self.__train_module

    @property
    def packages(self):
        return self.__packages

    @property
    def train_job_id(self):
        return self.__train_job_id

    @train_job_id.setter
    def train_job_id(self, train_job_id):
        self.__train_job_id = train_job_id

    @files_root.setter
    def files_root(self, files_root):
        self.__files_root = Path(files_root)

    @train_image_uri.setter
    def train_image_uri(self, train_image_uri):
        self.__train_image_uri = train_image_uri

    @custom_predictor_path.setter
    def custom_predictor_path(self, custom_predictor_path):
        self.__custom_predictor_path = self.__files_root / custom_predictor_path

    @artifacts.setter
    def artifacts(self, artifacts: []):
        self.__artifacts.extend(artifacts)

    @is_tuned.setter
    def is_tuned(self, __is_tuned):
        self.__is_tuned = __is_tuned

    @train_job_path.setter
    def train_job_path(self, train_job_path):
        self.__train_job_path = self.__files_root / train_job_path

    @model_uri.setter
    def model_uri(self, model_uri):
        self.__model_uri = model_uri

    @model_file.setter
    def model_file(self, model_file):
        self.__model_file = self.__files_root / model_file

    @version.setter
    def version(self, version):
        self.__version = version

    @name.setter
    def name(self, name):
        self.__name = name

    @packages.setter
    def packages(self, packages):
        self.__packages = packages

    @train_module.setter
    def train_module(self, train_module):
        self.__train_module = train_module

    @train_arguments.setter
    def train_arguments(self, train_arguments: Arguments):
        self.__train_arguments = train_arguments


class ModelBuilder:

    def __init__(self):
        self.reset()

    def reset(self):
        self.__artifacts = []
        self.__packages = []
        self.__train_arguments = []
        self.__model = Model()

    def files_root(self, files_root):
        self.__model.files_root = files_root
        return self

    def train_job_id(self, train_job_id):
        self.__model.train_job_id(train_job_id)
        return self

    def train_image_uri(self, train_image_uri):
        self.__model.train_image_uri = train_image_uri
        return self

    def model_file(self, model_file):
        self.__model.model_file = model_file
        return self

    def model_uri(self, model_uri):
        self.__model.model_uri = model_uri
        return self

    def custom_predictor_path(self, custom_predictor_path):
        self.__model.custom_predictor_path = custom_predictor_path
        return self

    def artifact(self, artifact: Artifact):
        artifact.file_path_from_root(self.__model.files_root)
        self.__artifacts.append(artifact)
        return self

    def artifacts(self, artifacts: [Artifact]):
        self.__model.artifacts = map(lambda x: x.file_path_from_root(self.__model.files_root), artifacts)
        return self

    def package(self, package: Artifact):
        self.__packages.append(package)
        return self

    def packages(self, packages: []):
        self.__packages = packages
        return self

    def train_argument(self, train_argument):
        self.__train_arguments.append(train_argument)
        return self

    def train_arguments(self, train_arguments: Arguments):
        self.__train_arguments.extend(train_arguments.get())
        return self

    def is_tuning(self, is_tuning):
        self.__model.is_tuned = is_tuning
        return self

    def version(self, version):
        self.__model.version = version
        return self

    def name(self, name):
        self.__model.name = name
        return self

    def build(self):
        self.__model.artifacts = self.__artifacts
        self.__model.train_arguments = self.__train_arguments
        self.__model.packages = map(lambda x: x.build(), self.__artifacts)

        model = self.__model

        self.reset()

        return model
