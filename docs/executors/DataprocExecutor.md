### Class DataprocExecutor(Executor)

**class com.griddynamics.dsl.ml.executors.executors.DataprocExecutor**

Class for running jobs on  Google Dataproc.

#### Properties

|property   | description                                                                           
|-----------|---------------------------------------------|
| __job   | [PysparkJob](https://github.com/griddynamics/ml-dsl/blob/master/docs/jobs/PySparkJob.md) job to run on Google Dataproc.|
| __session   | [Session](https://github.com/griddynamics/ml-dsl/blob/master/docs/sessions/Session.md) of a job on Google Dataproc.|
| job_status    | Status of a job on Google Dataproc.|
| __status_history| List  of job statuses on Google Dataproc.|
| __yarn_app| List of yarn statuses for a job on Google Dataproc.|
| __cluster_uuid| Unique id of cluster on Cloud Platform.|
| __job_description| Dictionary with information about running spark job.|
| __scheduling| |

#### Methods  
> **submit_job(run_async=True)**
> > Submitting a [PysparkJob](https://github.com/griddynamics/ml-dsl/blob/master/docs/jobs/PySparkJob.md) on Google Dataproc.
> 
> **__upload_files()**
> > Uploading job files to the directory on GCS.
> 
> **get_job()**
> > Getting a job on Google Dataproc.
> 
> **list(session: [CompositeSession](https://github.com/griddynamics/ml-dsl/blob/master/docs/sessions/CompositeSession.md), page_size=None, \*\*kwargs)**
> > Static method. Getting a list of jobs running on Google Dataproc.
> 
> **cancel_job()**
> > Cancelling a job on Google Dataproc.
> 
> **get_job_state()**
> > Get the status of a job running on Google Dataproc.
> 
> **__wait_for_job()**
> > Wait for a job is finished on Google Dataproc. 
> 
> **_print_job_status(status_history)**
> > Print statuses of a job running on Google Dataproc.
> 
> **_print_yarn_status(yarn_app_status)**
> > Print yarn statuses of a job running on Google Dataproc.
> 
> **download_output_from_gs()**
> > Download output of job from GCS.
> 
> **upload_file_to_gs_job_path(file_path)**
> > Uploading job file to GCS. file_path - local path to file for uploading.
> 
> **upload_script_to_gs_job_path(py_script: PyScript)**
> > Uploading [PyScript](https://github.com/griddynamics/ml-dsl/blob/master/docs/PyScript.md) (main file of pyspark job) to GCS.
> 
> **__file_path_to_gs_path(path)**
> > Return path of file on GCS. path - path on gs or file name.
> 
> **__py_script_to_gs_path(path)**
> > Return path of [PyScript](https://github.com/griddynamics/ml-dsl/blob/master/docs/PyScript.md) file on GCS. path - path on gs or PyScript name.
> 
> **__build_job_description()**
> > Building description of job running on Google Dataproc.
> 
> **job_description()**
> > Return description of job running on Google Dataproc.
