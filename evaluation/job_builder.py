from abc import ABC, abstractmethod
from google.cloud import dataproc_v1
from google.cloud.dataproc_v1.gapic.transports import (
    job_controller_grpc_transport)
from time import sleep
from google.cloud import storage
from datetime import datetime
from google.cloud.dataproc_v1.gapic import enums
import os
from enum import Enum


class ScriptState(Enum):
    UNDEFINED = 1
    DEFINED = 2


class PyScript:
    def __init__(self):
        self.__name = 'default.py'
        self.__script = None
        self.__package = None
        self.__state = ScriptState.UNDEFINED

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

    @name.setter
    def name(self, val: str):
        if val.endswith('.py'):
            self.__name = val
        else:
            self.__name = val + '.py'

    @state.setter
    def state(self, val: ScriptState):
        self.__state = val

    @package.setter
    def package(self, val):
        self.__package = val

    @script.setter
    def script(self, val):
        try:
            compile(val, '', exec)
            self.__script = val
            self.state = ScriptState.DEFINED
        except:
            print("Cannot compile script")


class JobBuilder(ABC):

    @property
    def job(self):
        pass

    @abstractmethod
    def job_file(self, val):
        pass

    @abstractmethod
    def py_script(self, val: PyScript):
        pass

    @abstractmethod
    def async(self):
        pass

    @abstractmethod
    def job_id(self, val):
        return self

    @abstractmethod
    def py_file(self, val):
        pass

    @abstractmethod
    def file(self, val):
        pass

    @abstractmethod
    def max_failures(self, val):
        pass

    @abstractmethod
    def arg(self, val):
        pass

    @abstractmethod
    def logging(self, val):
        pass

    @abstractmethod
    def archive(self, val):
        pass

    @abstractmethod
    def jar(self, val):
        pass

    @abstractmethod
    def property(self, val):
        pass


class DataprocJobBuilder(JobBuilder):

    def py_script(self, val: PyScript):
        self.__job.py_script = val
        return self

    def __init__(self):
        self.reset()

    def reset(self):
        self.__job = PySparkJob()

    def build_job(self):
        job = self.__job
        self.reset()
        return job

    def job_file(self, val):
        self.__job.job_file = val
        return self

    def async(self):
        self.__job.async = True
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

    def arg(self, val):
        self.__job.args = val
        return self


class PySparkJob:
    def __init__(self):
        self.__job_file = None
        self.__py_script = None
        self.__async = False
        self.__job_id = None
        self.__async = True
        self.__py_files = []
        self.__files = []
        self.__max_failures = 0
        self.__args = []
        self.__properties = {}
        self.__jars = []
        self.__archives = []
        self.__logging = None

    @property
    def job_file(self):
        return self.__job_file

    @property
    def py_script(self) -> PyScript:
        return self.__py_script

    @property
    def async(self):
        return self.__async

    @property
    def job_id(self):
        return self.__job_id

    @property
    def py_files(self):
        return self.__py_files

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
    def archives(self):
        return self.__archives

    @property
    def logging(self):
        return self.__logging

    @property
    def properties(self):
        return self.__properties

    @job_file.setter
    def job_file(self, value):
        self.__job_file = value

    @async.setter
    def async(self, val):
        self.__async = val

    @py_script.setter
    def py_script(self, job_script: PyScript):
        self.__py_script = job_script

    @job_id.setter
    def job_id(self, val):
        self.__job_id = val

    @max_failures.setter
    def max_failures(self, val):
        self.__max_failures = val

    @py_files.setter
    def py_files(self, val):
        self.__py_files.append(val)

    @files.setter
    def files(self, val):
        self.__files.append(val)

    @properties.setter
    def properties(self, val):
        self.properties.update(val)

    @archives.setter
    def archives(self, val):
        self.archives.append(val)

    @jars.setter
    def jars(self, val):
        self.jars.append(val)

    @args.setter
    def args(self, val):
        self.args.append(val)


class Session:
    def __init__(self, bucket, zone, cluster, project_id, job_path='jobs-root'):
        self.__bucket = bucket
        self.__jobs_path = job_path
        self.__zone = zone
        self.__cluster = cluster
        self.__project_id = project_id
        self.__region = None
        self.__cluster_uuid = None

        if self.zone == 'global':
            self.__region = self.zone
            self._dataproc_job_client = dataproc_v1.JobControllerClient()
        else:
            self.__region = self.get_region_from_zone(self.__zone)
            job_transport = (
                job_controller_grpc_transport.JobControllerGrpcTransport(
                    address='{}-dataproc.googleapis.com:443'.format(region)))
            self._dataproc_job_client = dataproc_v1.JobControllerClient(job_transport)

    @staticmethod
    def get_region_from_zone(zone):
        try:
            region_as_list = zone.split('-')[:-1]
            return '-'.join(region_as_list)
        except (AttributeError, IndexError, ValueError):
            raise ValueError('Invalid zone provided, please check your input.')

    @property
    def zone(self):
        return self.__zone

    @property
    def bucket(self):
        return self.__bucket

    @property
    def project_id(self):
        return self.__project_id

    @property
    def cluster(self):
        return self.__cluster

    @property
    def jobs_path(self):
        return self.__jobs_path

    @property
    def cluster_uuid(self):
        return self.__cluster_uuid

    @property
    def region(self):
        return self.__region

    @staticmethod
    def load_from_file(bucket_name, filename):
        pass


class Executor(ABC):

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


class DataprocExecutor(Executor):

    def __init__(self, job: PySparkJob, session: Session):
        self.__job = job
        self.__session = session
        self.job_status = None
        self.__status_history = []
        self.__yarn_app = []
        self.__cluster_uuid = None

    def submit_job(self, async=True):

        self.__upload_files()

        job = self.__session._dataproc_job_client.submit_job(self.__session.project_id, self.__session.region,
                                                             self.__job_description(), self.__job.job_id)
        print('Job with id {} was submitted to the cluster {}'.format(self.__job.job_id, self.__session.cluster))

        self.job_status = job.status.State.Name(job.status.state)
        self.__cluster_uuid = job.placement.cluster_uuid

        if async:
            return job
        else:
            return self.__wait_for_job()

    def __upload_files(self):
        if self.__job.py_script and self.__job.py_script.state == ScriptState.DEFINED:
            self.upload_string_to_gs_job_path(self.__job.py_script)
        else:
            self.upload_file_to_gs_job_path(self.__job.job_file)
        for jar in self.__job.jars:
            self.upload_file_to_gs_job_path(jar)
        for py in self.__job.py_files:
            self.upload_file_to_gs_job_path(py)
        for arch in self.__job.archives:
            self.upload_file_to_gs_job_path(arch)
        for f in self.__job.files:
            self.upload_file_to_gs_job_path(f)

    def get_job(self):

        job = self.__session._dataproc_job_client.get_job(self.__session.project_id, self.__session.region,
                                                          self.__job.job_id)
        self.job_status = job.status.State.Name(job.status.state)
        self.__status_history = job.status_history
        self.__yarn_app = job.yarn_applications

        return job

    def cancel_job(self):
        print("Canceling job: {}".format(self.__job.job_id))
        return self.__session._dataproc_job_client.cancel_job(self.__session.project_id, self.__session.region,
                                                              self.__job.job_id)

    def __wait_for_job(self):
        try:
            while True:

                job = self.__session._dataproc_job_client.get_job(self.__session.project_id, self.__session.region,
                                                                  self.__job.job_id)
                self.job_status = job.status.State.Name(job.status.state)

                self._print_job_status(job.status_history)

                self._print_yarn_status(job.yarn_applications)

                self.__status_history = job.status_history
                self.__yarn_app = job.yarn_applications

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
        if new_status_len > 0:
            for i in range(new_status_len):
                y = yarn_app_status[i]
                if i < cur_status_len:
                    old = self.__yarn_app[i]
                    if not (y.name == old.name and y.progress == old.progress and y.state == old.state
                            and y.tracking_url == old.tracking_url):
                        print('      Yarn APP {} with STATUS {} has PROGRESS {}'.format(y.name,
                                                                                        enums.YarnApplication.State(
                                                                                            y.state).name,
                                                                                        int(y.progress * 100)))
                else:
                    print('      Yarn APP {} with STATUS {} has PROGRESS {}'.format(y.name, enums.YarnApplication.State(
                        y.state).name,
                                                                                    int(y.progress * 100)))

    def download_output_from_gs(self):
        """Downloads the output file from Cloud Storage and returns it as a
        string."""
        print('Downloading output file.')
        client = storage.Client(project=self.__session.project_id)
        bucket = client.get_bucket(self.__session.bucket)
        output_blob = (
            ('google-cloud-dataproc-metainfo/{}/jobs/{}/driveroutput.000000000'.
             format(self.__cluster_uuid, self.__job.job_id)))
        return bucket.blob(output_blob).download_as_string()

    def upload_file_to_gs_job_path(self, filePath):
        """Uploads the PySpark file in this directory to the configured input
        bucket."""
        spark_file, spark_filename = get_file(filePath)
        client = storage.Client(project=self.__session.project_id)
        bucket = client.get_bucket(self.__session.bucket)
        blob = bucket.blob('{}/{}/{}'.format(self.__session.jobs_path, self.__job.job_id, spark_filename))
        blob.upload_from_file(spark_file)

    def upload_string_to_gs_job_path(self, py_script: PyScript):
        """Uploads the PySpark script in this directory to the configured input
        bucket."""
        client = storage.Client(project=self.__session.project_id)
        bucket = client.get_bucket(self.__session.bucket)
        blob = bucket.blob('{}/{}'.format(self.__session.jobs_path, py_script.name))
        blob.upload_from_string(py_script.script)

    def __file_path_to_gs_path(self, path):
        return 'gs://{}/{}/{}/{}'.format(self.__session.bucket, self.__session.jobs_path, self.__job.job_id,
                                         get_file_name(path))

    def __job_description(self):
        session = self.__session
        job = self.__job

        script_file = job.py_script.name if job.py_script and job.py_script.state == ScriptState.DEFINED else job.job_file

        return {
            "reference": {
                "project_id": session.project_id,
                "job_id": job.job_id
            },
            'placement': {
                'cluster_name': session.cluster
            },
            'pyspark_job': {
                'main_python_file_uri': self.__file_path_to_gs_path(script_file),
                'args': job.args,
                'python_file_uris': list(map(self.__file_path_to_gs_path, job.py_files)),
                'file_uris': list(map(self.__file_path_to_gs_path, job.files)),
                'jar_file_uris': list(map(self.__file_path_to_gs_path, job.jars)),
                'archive_uris': list(map(self.__file_path_to_gs_path, job.archives)),
                'logging_config': job.logging,
                'properties': job.properties
            }
        }

    def download_output(self):
        return self.download_output_from_gs()


def get_file(file):
    if file:
        f = open(file, "rb")
        return f, os.path.basename(file)


def get_file_name(path):
    return path.split('/')[-1]
