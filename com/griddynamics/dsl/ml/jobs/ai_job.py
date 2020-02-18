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

from com.griddynamics.dsl.ml.models.models import Model


class AIJob:
    def __init__(self):
        self.__name = None
        self.__train_input = {}
        self.__prediction_input = {}
        self.__deploy_input = {}
        self.__model = None
        self.__job_dir = None
        self.__package_src = None
        self.__package_dst = None

    @property
    def train_input(self):
        return self.__train_input

    @property
    def prediction_input(self):
        return self.__prediction_input

    @property
    def name(self):
        return self.__name

    @property
    def deploy_input(self):
        return self.__deploy_input

    @property
    def model(self):
        return self.__model

    @property
    def job_dir(self):
        return self.__job_dir

    @job_dir.setter
    def job_dir(self, job_dir):
        self.__job_dir = job_dir

    @property
    def package_src(self):
        return self.__package_src

    @package_src.setter
    def package_src(self, package_src):
        self.__package_src = package_src

    @property
    def package_dst(self):
        return self.__package_dst

    @package_dst.setter
    def package_dst(self, package_dst):
        self.__package_dst = package_dst

    @model.setter
    def model(self, model: Model):
        self.__model = model

    @deploy_input.setter
    def deploy_input(self, deploy_input):
        self.__deploy_input = deploy_input

    @train_input.setter
    def train_input(self, train_input):
        self.__train_input = train_input

    @name.setter
    def name(self, name):
        self.__name = name


class AIJobBuilder:
    def __init__(self):
        self.reset()

    def reset(self):
        self.__job = AIJob()

    def name(self, name):
        self.__job.name = name
        return self

    def model(self, model: Model):
        self.__job.model = model
        return self

    def train_input(self, train_input):
        self.__job.train_input = train_input
        return self

    def deploy_input(self, deploy_input):
        self.__job.deploy_input = deploy_input
        return self

    def load_hyperparameters_from_file(self, config_path):
        document = open(config_path, 'r')
        # noinspection PyUnresolvedReferences
        self.__job.train_input['hyperparameters'] = yaml.load(document, yaml.FullLoader)
        self.__job.model.is_tuned = True
        return self

    def hyperparameter(self, parameter: str):
        if self.__job.train_input['hyperparameters']:
            self.__job.train_input['hyperparameters']['params'].append(parameter)
        else:
            params = {'params': [parameter]}
            self.__job.train_input['hyperparameters'] = params
        self.__job.model.is_tuned = True
        return self

    def job_dir(self, job_dir):
        self.__job.job_dir = job_dir
        return self

    def package_src(self, package_src):
        self.__job.package_src = package_src
        return self

    def package_dst(self, package_dst):
        self.__job.package_dst = package_dst
        return self

    def build(self):

        job = self.__job

        model = self.__job.model

        job.train_input['jobDir'] = job.job_dir

        if model:
            job.train_input['args'] = model.train_arguments

        if model:
            if model.train_image_uri:
                job.train_input['masterConfig'] = {'imageUri': model.train_image_uri}
            elif model.train_module and model.packages:
                job.train_input['packageUris'] = map(lambda x: x.build, model.packages)

        self.reset()

        return job
