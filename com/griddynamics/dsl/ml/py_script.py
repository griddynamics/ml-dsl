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

from com.griddynamics.dsl.ml.settings.description import ScriptState


class PyScript:
    """Python script model.

        :type __script: : str
        :params __script: Python script for execution. Must contains __main__ method

        :type __name: str
        :params __name: (optional) name of the script.

        :type __state: :class:`ScriptState`
        :params __state: (optional) State of the script

        :type __package: str
        :params __package: (optional) package of the script.

        :type __class_name: str
        :params __class_name: (optional) name of class from the script which is inherited from Task
    """
    __script = None
    __package = None
    __name = None
    __class_name = None

    def __init__(self, script, name='default.py', class_name=None):
        self.name = name
        self.script = script
        self.__state = ScriptState.DEFINED if self.script else ScriptState.UNDEFINED
        self.__class_name = class_name

    @property
    def name(self):
        return self.__name

    @property
    def script(self):
        return self.__script

    @property
    def package(self):
        return self.__package

    @property
    def state(self):
        return self.__state

    @property
    def class_name(self):
        return self.__class_name

    @name.setter
    def name(self, val: str):
        self.__name = val

    @state.setter
    def state(self, val: ScriptState):
        self.__state = val

    @package.setter
    def package(self, val):
        self.__package = val

    @script.setter
    def script(self, val):
        self.__script = val
        self.__state = ScriptState.DEFINED

