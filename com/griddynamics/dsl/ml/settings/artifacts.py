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

from com.griddynamics.dsl.ml.helpers import *


class Artifact:
    def __init__(self, file_name: str, mapping=None, path=''):
        self.__file_name = file_name
        self.__mapping = mapping
        self.__path = path

    @property
    def file_name(self):
        return str(self.__file_name)

    @property
    def mapping(self):
        return self.__mapping

    @property
    def path(self):
        return self.__path

    def build(self):
        return GCPHelper.construct_path(Helper.get_file_name(self.__mapping if self.__mapping else self.__file_name),
                                        self.__path)

    def file_path_from_root(self, file_root):
        if not self.__file_name.startswith("gs://"):
            self.__file_name = Path(file_root) / self.__file_name
        return self
