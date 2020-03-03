from time import sleep

from com.griddynamics.dsl.ml.jobs.pyspark_job import PySparkJob
from com.griddynamics.dsl.ml.sessions import Session
from com.griddynamics.dsl.ml.helpers import *


class JobValidator:
    def __init__(self, job: PySparkJob, session: Session, wait_delay=5, wait_tries=5):
        self.__job = job
        self.__session = session
        self.wait_delay = wait_delay
        self.wait_tries = wait_tries

    def wait_for_new_job_is_approved(self, validation_pattern):
        try:
            tries = self.wait_tries
            while tries > 0:
                blobs = self.list_blobs()
                for blob in blobs:
                    read_output = blob.download_as_string().decode('utf-8')
                    if validation_pattern not in read_output:
                        pass
                    else:
                        print('Validation of new version job is successful')
                        return True
                tries -= 1
                sleep(self.wait_delay)
            return False
        except KeyboardInterrupt:
            return False

    def list_blobs(self):
        job_logs = self.__job.driver_output_resource_uri
        prefix = job_logs.replace('gs://{}/'.format(self.__session.bucket), '')
        storage_client = storage.Client(project=self.__session.project_id)
        source_bucket = storage_client.get_bucket(self.__session.bucket)
        blobs = source_bucket.list_blobs(prefix=prefix)
        return list(blobs)

