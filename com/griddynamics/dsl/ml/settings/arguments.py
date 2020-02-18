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

from functools import reduce


class Arguments:
    def __init__(self):
        self.__arguments = {}

    def set_arg(self, key, value):
        self.__arguments[key] = value

    def set_args(self, **kwargs):
        self.__arguments.update(kwargs)

    def get_arg(self, key: str):
        return self.__arguments[key]

    def get(self):
        return list(reduce(lambda x, y: x + y, self.__arguments.items()))
