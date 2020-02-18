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

from enum import Enum


class ScriptState(str, Enum):
    UNDEFINED = 1
    DEFINED = 2


class Platform(str, Enum):
    GCP = 1
    AWS = 2


class ComponentType(str, Enum):
    SPARK_JOB = 1
    AI_JOB = 2
    MODEL = 3
