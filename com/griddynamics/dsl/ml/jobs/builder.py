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

from com.griddynamics.dsl.ml.settings.arguments import Arguments
from com.griddynamics.dsl.ml.jobs.pyspark_job import PySparkJob


class DataprocJobBuilder:
    """Job builder implementation for Dataproc jobs
    """
    def files_root(self, files_root: str):
        self.__job.files_root = files_root
        return self

    def task_script(self, name: str, py_scripts: dict):
        script = py_scripts[name]
        if script is None:
            raise ValueError(f"There is no defined script with name: {name}")

        #         if script.class_name is None:
        #             raise ValueError('Script for execution should be defined with --task flag')
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

    def __init__(self):
        self.reset()

    def reset(self):
        self.__job = PySparkJob()

    def build_job(self, profile):
        if not (self.__job.job_file or self.__job.task_script):
            raise ValueError("Script for execution wasn't set")

        job = self.__job.from_profile(profile)
        self.reset()
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

    def property(self, key, val):
        self.__job.properties = {key: val}
        return self

    def archive(self, val):
        self.__job.archives = val
        return self

    def label(self, key, val):
        self.__job.labels = key, val
        return self

