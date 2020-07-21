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


from com.griddynamics.dsl.ml.mldsl import DataprocExecutor
from com.griddynamics.dsl.ml.settings.description import ComponentType
from com.griddynamics.dsl.ml.sessions import CompositeSession
from com.griddynamics.dsl.ml.jobs.pyspark_job import PySparkJob
import json
from json import JSONEncoder
from pathlib import Path
from datetime import datetime


class Component(JSONEncoder):
    def __init__(self, id, type: ComponentType = None, env=None, configuration=None, state='STATE_UNSPECIFIED',
                 parameters=None,
                 name=None, *children):
        self.__name = name
        self.__type = type
        self.__env = env
        self.__parameters = parameters
        self.__id = id
        self.__configuration = configuration
        self.__configuration.job_id = id
        self.__state = state
        self.__child_components = children
        self.__labels = None

    @property
    def id(self):
        return self.__id

    @property
    def type(self):
        return self.__type

    @property
    def env(self):
        return self.__env

    @property
    def configuration(self):
        return self.__configuration

    @property
    def state(self):
        return self.__state

    @property
    def name(self):
        return self.__name

    @property
    def labels(self):
        return self.__labels

    def default(self, obj):
        return \
            {
                'id': obj.id,
                'env': obj.env,
                'state': obj.state,
                'name': obj.name,
                'type': obj.type.name,
                'configuration': obj.configuration
            }

    def to_json(self):
        return json.dumps(self,
                          default=self.default,
                          indent=4)

    @labels.setter
    def labels(self, val):
        self.__labels = val

    @state.setter
    def state(self, state):
        self.__state = state


class Environment(JSONEncoder):

    def __init__(self, name, session: CompositeSession, env=None, json_cnfg=None):
        self.__session = session
        self.__name = name
        self.__components = []
        self.__env = env
        self.__json = json.loads(json_cnfg)
        self.__state = 'STATE_UNSPECIFIED'

    @property
    def components(self):
        return self.__components

    @property
    def json(self):
        return self.__json

    @property
    def name(self):
        return self.__name

    @property
    def state(self):
        return self.__state

    @property
    def env(self):
        return self.__env

    def add_component(self, component):
        self.__components.append(component)

    def load(self):
        if self.__components:
            for c in self.__components:
                executor = DataprocExecutor(c.configuration, self.__session)
                try:
                    cur_state = executor.get_job_state()
                    c.state = cur_state
                except:
                    c.state = 'STATE_UNSPECIFIED'

        elif self.__env:
            default_filter = {'labels.env': self.__env}
            self._load_custom_filter(default_filter)
        elif self.__json and self.__json.get('components'):
            for c in self.__json['components']:
                job = PySparkJob()
                job.from_json(c['configuration'])

                executor = DataprocExecutor(job, self.__session)
                try:
                    cur_state = executor.get_job_state()
                except:
                    cur_state = 'STATE_UNSPECIFIED'

                new_id = f"{c.get('id')[0: c.get('id').rindex('_')]}_{int(datetime.now().timestamp())}"
                job.job_id = new_id

                self.__components.append(
                    Component(new_id, ComponentType.SPARK_JOB, self.__name, configuration=job, state=cur_state))
        else:
            raise ValueError("Please set up one of env or json property")

    def _load_custom_filter(self, **filters):
        jobs = DataprocExecutor.list(self.__session, 10, **filters)
        self.__fill_components_from_dp_jobs(jobs)

    def __fill_components_from_dp_jobs(self, jobs: []):
        for j in jobs:
            c = Component(j.reference.job_id, ComponentType.SPARK_JOB, self.__env, j.pyspark_job,
                          j.status.State.Name(j.status.state))
            if j.labels:
                c.labels = j.labels
            self.__components.append(c)

    def start(self):
        if self.__json:
            for c in self.__components:
                if c.state not in ['PENDING', 'RUNNING', 'SETUP_DONE', 'ATTEMPT_FAILURE']:
                    executor = DataprocExecutor(c.configuration, self.__session).submit_job()
                    c.state = executor.job_status

    def stop(self):
        pass

    def default(self, obj):
        return {'env': obj.env,
                'state': obj.state,
                'name': obj.name,
                'json': '...',
                'components': obj.components
                }

    def to_json(self):
        return json.dumps(self,
                          default=lambda o: o.default(o) if isinstance(o, JSONEncoder) else JSONEncoder.default(o),
                          indent=4)

    def __str__(self):
        return self.to_json()

    def save_as_local_file(self, path):
        out = self.to_json()
        p = Path(path)
        p.write_text(out)
