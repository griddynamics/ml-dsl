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

import json
import shutil

from abc import ABC, abstractmethod
from time import sleep
from distutils.core import run_setup
from googleapiclient import errors
from datetime import datetime
# noinspection PyUnresolvedReferences
from google.cloud.dataproc_v1.gapic import enums
from functools import reduce

from com.griddynamics.dsl.ml.jobs.pyspark_job import PySparkJob
from com.griddynamics.dsl.ml.jobs.ai_job import AIJob
from com.griddynamics.dsl.ml.sessions import Session, CompositeSession
from com.griddynamics.dsl.ml.py_script import PyScript, ScriptState
from com.griddynamics.dsl.ml.helpers import *


class Executor(ABC):
    """Abstract executor class"""

    @abstractmethod
    def submit_job(self, job: PySparkJob, session: Session):
        pass

    @abstractmethod
    def get_job(self):
        pass

    @abstractmethod
    def cancel_job(self):
        pass

    @abstractmethod
    def download_output(self):
        pass

    @abstractmethod
    def get_job_state(self):
        pass


class AIPlatformJobExecutor:
    def __init__(self, session: CompositeSession, ai_job: AIJob = None, wait_delay=5, wait_tries=5):
        self.session = session.get_ml_session()
        self.ai_job = ai_job
        self.project_id = f'projects/{self.session.project_id}'
        self.__request = None
        self.wait_delay = int(wait_delay)
        self.wait_tries = int(wait_tries)

        if self.ai_job.train_input:
            self.ai_job.train_input['region'] = self.session.zone
        self.use_cloud_engine_credentials = session.use_cloud_engine_credentials

    def package(self):
        try:
            work_dir = str(Path.cwd())
            os.chdir(self.ai_job.package_src)
            run_setup('setup.py', script_args=['sdist', '--dist-dir=dist', '--format=gztar'])
            os.chdir(work_dir)
            return True
        except Exception as e:
            print(str(e))
            return False

    def upload_package(self):
        dist_dir = '{}/dist'.format(self.ai_job.package_src)
        package_uris = []
        try:
            for file_name in os.listdir(dist_dir):
                if file_name.endswith('.tar.gz'):
                    artifact = os.path.basename(file_name)
                    artifact_src = '{}/{}'.format(dist_dir, artifact)
                    GCPHelper.upload_file_to_storage(self.session.project_id, self.session.bucket,
                                                     artifact_src, self.ai_job.package_dst,
                                                     use_cloud_engine_credentials=self.use_cloud_engine_credentials)
                    staging_dir = 'gs://{}/{}/{}'.format(self.session.bucket, self.ai_job.package_dst, artifact)
                    print(f"Uploading custom package {artifact_src} to dir {staging_dir}")
                    package_uris.append(staging_dir)
        except Exception as e:
            print(str(e))
        return package_uris

    def submit_train_job(self):
        train_input = self.ai_job.train_input

        model_files_path = self.ai_job.model.files_root
        if self.ai_job.package_src:
            if self.package():
                self.ai_job.train_input['packageUris'] = self.upload_package()

        for p in self.ai_job.model.packages:
            if str(Path(model_files_path) / p.file_name).startswith("gs://"):
                GCPHelper.copy_file_on_storage(self.session.bucket, p.file_name, p.build(),
                                               use_cloud_engine_credentials=self.use_cloud_engine_credentials)
            else:
                GCPHelper.upload_file_to_storage(self.session.project_id, self.session.bucket, p.file_name, p.build(),
                                                 use_cloud_engine_credentials=self.use_cloud_engine_credentials)

        job_spec = {'jobId': self.ai_job.name, 'trainingInput': train_input}

        request = self.session._cloudml.projects().jobs().create(body=job_spec, parent=self.project_id)

        self.__request = request

        request.execute()
        job_name = f'projects/{self.session.project_id}/jobs/{self.ai_job.name}'
        statuses = ['SUCCEEDED', 'FAILED', 'CANCELLED']
        response = self.__wait_for_job(self.get_job, job_name,
                                       self.wait_tries,
                                       self.wait_delay, self.cancel_job, *statuses)

        return response

    def submit_batch_prediction_job(self):
        job_spec = {'jobId': self.ai_job.name, 'predictionInput': self.ai_job.prediction_input}

        request = self.session._cloudml.projects().jobs().create(body=job_spec, parent=self.project_id)

        self.__request = request

        response = request.execute()
        return response

    @staticmethod
    def cancel_job(session: Session, job_name=None):
        ml = session._cloudml
        if job_name:
            # noinspection PyUnresolvedReferences
            request = ml.projects().jobs().cancel(name=f'{self.project_id}/jobs/{job_name}')
        else:
            # noinspection PyUnresolvedReferences
            request = ml.projects().jobs().cancel(name=f'{self.project_id}/jobs/{self.ai_job.name}')

        return request.execute()

    def create_model(self, model_name, online_prediction_logging=True):
        body = {
            'name': model_name,
            'regions': self.session.zone,
            'onlinePredictionLogging': online_prediction_logging
        }
        request = self.session._cloudml.projects().models().create(parent=f'projects/{self.session.project_id}',
                                                                   body=body)

        self.__request = request

        return request.execute()

    def submit_deploy_model_job(self, version_name, train_job_id=None, objective_value_is_maximum_needed=False,
                                create_new_model=False):
        version_body = self.ai_job.deploy_input

        model_name = self.ai_job.model.name

        if self.ai_job.package_src:
            if self.package():
                version_body['packageUris'] = self.upload_package()

        custom_predictor_path = str(self.ai_job.model.custom_predictor_path)
        if self.ai_job.model.custom_predictor_path:
            """
             - Build package with custom predictor
             - Copy gztar package to GS
             - Clean local files
            """

            version = '1.0'

            p = Path(custom_predictor_path)
            file_name = p.name
            name = file_name.replace(".py", "")

            path_to_file = custom_predictor_path.replace(file_name, "")
            work_dir = str(Path.cwd())
            os.chdir(path_to_file)
            # GCPHelper.build_package(name, [file_name], script_args, version, requires)
            run_setup('setup.py', script_args=['sdist', '--dist-dir=dist', '--format=gztar'])
            os.chdir(work_dir)

            pkg_name = f'{name}-{version}.tar.gz'

            pkg_path = str(Path(f'{path_to_file}/dist/{pkg_name}'))

            staging_uri = f"{version_body['deploymentUri']}/staging"
            GCPHelper.upload_file_to_storage(self.session.project_id, self.session.bucket, pkg_path,
                                             staging_uri,
                                             use_cloud_engine_credentials=self.use_cloud_engine_credentials)
            print(f"Uploading file {pkg_name} to dir: {staging_uri}")

            version_body['packageUris'] = [f"{staging_uri}/{pkg_name}"]

            shutil.rmtree(str(Path(f'{path_to_file}/dist')))
            shutil.rmtree(str(Path(f'{path_to_file}/{name}.egg-info')))

        if create_new_model:
            self.create_model(model_name)

        # Use best model of HP tuning job
        if self.ai_job.model.is_tuned:
            if train_job_id is None:
                train_job_id = self.ai_job.model.train_job_id

            if train_job_id is None:
                raise ValueError("train_job_id must be specified for tuning job")
            best_trial = self.get_best_hp_tuning_result(self.session, train_job_id, objective_value_is_maximum_needed)
            path = self.set_and_get_best_model_path(best_trial, self.session.bucket, f"{version_body['deploymentUri']}",
                                                    use_cloud_engine_credentials=self.use_cloud_engine_credentials)

            print(path)
            version_body['deploymentUri'] = path

        """ Copy artifacts to the deployment path. """
        for a in self.ai_job.model.artifacts:
            if a.file_name.startswith("gs://"):
                new_blob_name = os.path.join(version_body['deploymentUri'], Path(a.file_name).name)
                if Path(a.file_name).is_dir():
                    GCPHelper.copy_folder_on_storage(self.session.bucket, a.file_name, new_blob_name,
                                                     use_cloud_engine_credentials=self.use_cloud_engine_credentials)
                else:
                    print(f"Copying file {a.file_name} to: {new_blob_name}")
                    GCPHelper.copy_file_on_storage(self.session.bucket, a.file_name, new_blob_name,
                                                   use_cloud_engine_credentials=self.use_cloud_engine_credentials)
            else:
                print(f"Uploading file {a.file_name} to: {version_body['deploymentUri']}")
                GCPHelper.upload_file_to_storage(self.session.project_id, self.session.bucket, a.file_name,
                                                 version_body['deploymentUri'],
                                                 use_cloud_engine_credentials=self.use_cloud_engine_credentials)

        version_body['name'] = version_name

        request = self.session._cloudml.projects().models().versions().create(
            parent=f'{self.project_id}/models/{model_name}', body=version_body)

        self.__request = request

        request.execute()

        v_name = f'{self.project_id}/models/{model_name}/versions/{version_name}'

        response = self.__wait_for_job(self.get_version, v_name, self.wait_tries, self.wait_delay,
                                       'SUCCESS', 'READY', 'FAILED')

        return response

    def submit_prediction_job(self, predictions):
        name = predictions["name"]

        instances = predictions["instances"]
        response = self.session._cloudml.projects().predict(name=name,
                                                            body={"instances": instances}).execute()

        if "error" in response:
            raise RuntimeError(response['error'])

        if 'output_path' in predictions:
            with open(predictions['output_path'], 'w') as f:
                json.dump(response, f)

        return response['predictions']

    @staticmethod
    def get_job(session: Session, full_job_name: str):
        ml = session._cloudml
        request = ml.projects().jobs().get(name=full_job_name)
        return request.execute()

    @staticmethod
    def get_version(session: Session, version_full_name: str):
        ml = session._cloudml
        request = ml.projects().models().versions().get(name=version_full_name)
        return request.execute()

    @staticmethod
    def get_best_hp_tuning_result(session: CompositeSession, job_name, objective_value_is_maximum_needed=False,                                  debug=False):
        print('MAX is needed: {}'.format(objective_value_is_maximum_needed))

        try:
            job = AIPlatformJobExecutor.get_job(session, f'projects/{session.project_id}/jobs/{job_name}')
        except errors.HttpError as err:
            print(err)
        if debug:
            print('Job status for {}.{}:'.format(session.project_id, job_name))
            print('    state : {}'.format(job['state']))
            print('    consumedMLUnits : {}'.format(job['trainingOutput']['consumedMLUnits']))
            print('    completedTrialCount : {}'.format(['trainingOutput']['completedTrialCount']))
            print('    maxTrials: {}'.format(job['trainingInput']['hyperparameters']['maxTrials']))

            print('-' * 100)
            print("General Training Arguments:")
            print(json.dumps(job.get("trainingInput").get("args"), indent=2))

        trials_res = job.get("trainingOutput").get("trials")
        best_trial = sorted(trials_res,
                            key=lambda trial_res: trial_res.get("finalMetric", {}).get("objectiveValue", 1e6),
                            reverse=objective_value_is_maximum_needed)[0]
        if debug:
            print('-' * 100)
            print("Best Trial: ")
            print(json.dumps(best_trial, indent=2))

        return best_trial['trialId']

    @staticmethod
    def set_and_get_best_model_path(best_trial, bucket, train_job_dir, use_cloud_engine_credentials):
        GCPHelper.delete_path_from_storage(bucket, f"{train_job_dir}/model",
                                           use_cloud_engine_credentials=use_cloud_engine_credentials)
        best_model_dir = f"{train_job_dir}/model_trial_{best_trial}"
        print(f"Best trial path:{best_model_dir}")
        GCPHelper.copy_folder_on_storage(bucket, best_model_dir, f"{train_job_dir}/model",
                                         use_cloud_engine_credentials=use_cloud_engine_credentials)

        return best_model_dir

    def __wait_for_job(self, get_funk, name, wait_tries=5, delay=5, cancel_funk=None, *stop_statuses):
        try:
            tries = wait_tries
            job = None
            while tries > 0:
                job = get_funk(self.session, name)
                if job['state'] in stop_statuses:
                    return job
                tries -= 1
                sleep(delay)
            return job
        except KeyboardInterrupt:
            if cancel_funk:
                AIPlatformJobExecutor.cancel_job(self.session, name)


class DataprocExecutor(Executor):
    """Implementation of executor for Dataproc"""

    def __init__(self, job: PySparkJob, session: CompositeSession):
        self.__job = job
        self.__session = session.get_job_session()
        self.job_status = 'STATE_UNSPECIFIED'
        self.__status_history = []
        self.__yarn_app = []
        self.__cluster_uuid = None
        self.__job_description = None
        self.__scheduling = {'max_failures_per_hour': job.max_failures}
        self.use_cloud_engine_credentials = session.use_cloud_engine_credentials

    def submit_job(self, run_async=True):

        self.__upload_files()

        job = self.__session._dataproc_job_client.submit_job(self.__session.project_id, self.__session.region,
                                                             self.__build_job_description(), self.__job.job_id)
        print('Job with id {} was submitted to the cluster {}'.format(self.__job.job_id, self.__session.cluster))

        self.job_status = job.status.State.Name(job.status.state)
        self.__cluster_uuid = job.placement.cluster_uuid

        if run_async:
            while (len(self.__yarn_app) == 0) or (self.job_status != 'RUNNING'):
                job = self.get_job()
                sleep(1)
            return job
        else:
            return self.__wait_for_job()

    def __upload_files(self):
        if self.__job.job_file:
            self.upload_file_to_gs_job_path(self.__job.job_file)

        for jar in self.__job.jars:
            self.upload_file_to_gs_job_path(jar)
        for py in self.__job.py_files:
            self.upload_file_to_gs_job_path(py)
        for arch in self.__job.archives:
            self.upload_file_to_gs_job_path(arch)
        for f in self.__job.files:
            self.upload_file_to_gs_job_path(f)
        for s in self.__job.py_scripts:
            self.upload_script_to_gs_job_path(s)

    def get_job(self):

        job = self.__session._dataproc_job_client.get_job(self.__session.project_id, self.__session.region,
                                                          self.__job.job_id)
        self.job_status = job.status.State.Name(job.status.state)
        self.__status_history = job.status_history
        self.__yarn_app = job.yarn_applications

        return job

    @staticmethod
    def list(session: CompositeSession, page_size=None, **kwargs):
        session = session.get_job_session()

        def concat_func(x, y):
            return x + f'{y[0]}={y[1]} '

        items = list(kwargs.items())
        init = '' if not items else f'{items[0][0]}={items[0][1]} '
        filter = str(reduce(lambda x, y: concat_func(x, y), items[1:], init)).strip()

        if page_size:
            res = []
            for page in session._dataproc_job_client.list_jobs(session.project_id, region=session.region,
                                                               page_size=page_size, cluster_name=session.cluster,
                                                               filter_=filter).pages:
                res += page
        else:
            res = session._dataproc_job_client.list_jobs(session.project_id, session.region,
                                                         cluster_name=session.cluster,
                                                         filter_=filter)
        return res

    def get_job_state(self):
        job = self.__session._dataproc_job_client.get_job(self.__session.project_id, self.__session.region,
                                                          self.__job.job_id)
        self.job_status = job.status.State.Name(job.status.state)

        return self.job_status

    def cancel_job(self):
        print("Canceling job: {}".format(self.__job.job_id))
        self.__session._dataproc_job_client.cancel_job(self.__session.project_id, self.__session.region,
                                                              self.__job.job_id)
        while True:
            self.job_status = self.get_job_state()
            if self.job_status != 'CANCELLED':
                sleep(1)
            else:
                print('Job {} was successfully cancelled.'.format(self.__job.job_id))
                break

    def __wait_for_job(self):
        try:
            while True:
                job = self.get_job()
                self._print_job_status(job.status_history)
                self._print_yarn_status(job.yarn_applications)

                if self.job_status == 'ERROR':
                    print('Job is failed. \n Details: {}'.format(job.status.details))
                    raise Exception(job.status.details)
                elif self.job_status == 'DONE':
                    print('Job STATUS was set to {} at {}'.format(self.job_status,
                                                                  datetime.fromtimestamp(
                                                                      job.status.state_start_time.seconds)))
                    return job
                sleep(1)

        except KeyboardInterrupt:
            self.cancel_job()

    def _print_job_status(self, status_history):
        cur_hist_len = len(self.__status_history)
        new_hist_len = len(status_history)
        for s in status_history[cur_hist_len: new_hist_len]:
            print('Job STATUS was set to {} at {}'.format(enums.JobStatus.State(s.state).name,
                                                          datetime.fromtimestamp(s.state_start_time.seconds)))

    def _print_yarn_status(self, yarn_app_status):
        cur_status_len = len(self.__yarn_app)
        new_status_len = len(yarn_app_status)
        name = self.__job.task_script.name if self.__job.task_script is not None and \
                                              self.__job.task_script.state.value == ScriptState.DEFINED.value \
            else self.__job.job_file
        if new_status_len > 0:
            for i in range(new_status_len):
                y = yarn_app_status[i]
                if i < cur_status_len:
                    old = self.__yarn_app[i]
                    if not (y.name == old.name and y.progress == old.progress and y.state == old.state
                            and y.tracking_url == old.tracking_url):
                        print('      Yarn APP {} with STATUS {} has PROGRESS {}'.format(name,
                                                                                        enums.YarnApplication.State(
                                                                                            y.state).name,
                                                                                        int(y.progress * 100)))
                else:
                    print('      Yarn APP {} with STATUS {} has PROGRESS {}'.format(name, enums.YarnApplication.State(
                        y.state).name,
                                                                                    int(y.progress * 100)))

    def download_output_from_gs(self):
        """Downloads the output file from Cloud Storage and returns it as a
        string."""
        print('Downloading output file.')
        credentials = None
        if self.use_cloud_engine_credentials:
            credentials = compute_engine.Credentials()

        client = storage.Client(project=self.__session.project_id, credentials=credentials)
        bucket = client.get_bucket(self.__session.bucket)
        output_blob = (
            ('google-cloud-dataproc-metainfo/{}/jobs/{}/driveroutput.000000000'.
             format(self.__cluster_uuid, self.__job.job_id)))
        return bucket.blob(output_blob).download_as_string()

    def upload_file_to_gs_job_path(self, file_path):
        """Uploads the PySpark file in this directory to the configured input
        bucket."""
        if file_path and not str(file_path).startswith("gs://"):
            print(
                f"Uploading file to dir: {self.__session.jobs_path}/{self.__job.job_id}/"
                f"{Helper.get_file_name(file_path.name)}")
            GCPHelper.upload_file_to_storage(self.__session.project_id, self.__session.bucket, str(file_path),
                                             f'{self.__session.jobs_path}/{self.__job.job_id}',
                                             use_cloud_engine_credentials=self.use_cloud_engine_credentials)

    def upload_script_to_gs_job_path(self, py_script: PyScript):
        """Uploads the PySpark script in this directory to the configured input
        bucket."""

        credentials = None
        if self.use_cloud_engine_credentials:
            credentials = compute_engine.Credentials()
        client = storage.Client(project=self.__session.project_id, credentials=credentials)
        bucket = client.get_bucket(self.__session.bucket)
        blob = bucket.blob('{}/{}/{}'.format(self.__session.jobs_path, self.__job.job_id, py_script.name))
        blob.upload_from_string(py_script.script)

    def __file_path_to_gs_path(self, path):
        return path if str(path).startswith("gs://") else 'gs://{}/{}/{}/{}'.format(self.__session.bucket,
                                                                                    self.__session.jobs_path,
                                                                                    self.__job.job_id,
                                                                                    Helper.get_file_name(str(path)))

    def __py_script_to_gs_path(self, script):
        return 'gs://{}/{}/{}/{}'.format(self.__session.bucket, self.__session.jobs_path, self.__job.job_id,
                                         script.name)

    def __build_job_description(self):
        session = self.__session
        job = self.__job
        self.__job_description = {
            "reference": {
                "project_id": session.project_id,
                "job_id": job.job_id
            },
            'placement': {
                'cluster_name': session.cluster
            },
            'labels': self.__job.labels,
            'pyspark_job': {
                'main_python_file_uri': self.__file_path_to_gs_path(
                    self.__job.job_file) if self.__job.job_file else self.__file_path_to_gs_path(
                    self.__job.task_script.name),
                'args': self.__job.args,
                'python_file_uris': list(map(self.__file_path_to_gs_path, job.py_files)) + list(
                    map(self.__py_script_to_gs_path, job.py_scripts)),
                'file_uris': list(map(self.__file_path_to_gs_path, job.files)),
                'jar_file_uris': list(map(self.__file_path_to_gs_path, job.jars)),
                'archive_uris': list(map(self.__file_path_to_gs_path, job.archives)),
                'logging_config': job.logging,
                'properties': job.properties
            },
            'scheduling': self.__scheduling,
        }
        return self.__job_description

    def job_description(self):
        return self.__build_job_description()

    def download_output(self):
        return self.download_output_from_gs()


class JobUpgradeExecutor(DataprocExecutor):
    def __init__(self, job: PySparkJob, session: CompositeSession, old_job_id: str):
        super(JobUpgradeExecutor, self).__init__(job, session)
        self.__old_job_id = old_job_id

    def get_old_job_state(self):
        job = self._DataprocExecutor__session._dataproc_job_client.get_job(self._DataprocExecutor__session.project_id,
                                                                           self._DataprocExecutor__session.region,
                                                                           self.__old_job_id)
        old_job_status = job.status.State.Name(job.status.state)
        return old_job_status

    def cancel_old_job(self):
        print("Canceling job: {}".format(self.__old_job_id))
        self._DataprocExecutor__session._dataproc_job_client.cancel_job( self._DataprocExecutor__session.project_id,
                                                                             self._DataprocExecutor__session.region,
                                                                             self.__old_job_id)
        while True:
            old_job_status = self.get_old_job_state()
            if old_job_status != 'CANCELLED':
                sleep(1)
            else:
                print('Job {} was successfully cancelled.'.format(self.__old_job_id))
                break

    def get_old_job(self):
        job = self._DataprocExecutor__session._dataproc_job_client.get_job(
            self._DataprocExecutor__session.project_id,
            self._DataprocExecutor__session.region,
            self.__old_job_id)
        return job

    def submit_upgrade_job(self, validator, validator_path, run_async=True):
        job = self.submit_job(run_async=run_async)
        validator_class = validator_path[validator](job, self._DataprocExecutor__session)
        if validator_class.validate():
            print('Validation of new version job is successful')
            self.cancel_old_job()
            return job
        else:
            print("New job version doesn't meet the conditions or is invalid. Current working job version is {}. "
                  "Logs of failed job are in {}".
                  format(self.__old_job_id, job.driver_output_resource_uri))
            self.cancel_job()
            return self.get_old_job()


