### Class EmrExecutor(Executor)

**class com.griddynamics.dsl.ml.executors.executors.EmrExecutor**

Class for running spark jobs on EMR AWS. Inherited from Executor.

#### Properties

|property   | description                                                                           
|-----------|---------------------------------------------|
| __job   | PysparkJob job to run on an Emr cluster.|
| __session   | Session of a job on Emr cluster.|
| __client    | Emr client to interact with the Emr cluster. Defined using Boto3 python library.|
| __yarn_app| List of yarn statuses for a job on Emr AWS.|
| __cluster_uuid| Id of Emr cluster.|
| __source_emr| Path where application artifacts and their configuration directories are installed.|
| __step_ids| List of steps to run on EMR cluster.|
| __job_description| Python dictionary of the EMR  job description.|
| job_status| Python dictionary with states of job flow steps.|

#### Methods  
> **retry_on_throttling(exc)**
> > Decorator utility function. Retry on Throttling Exception of calling Emr cluster.
> 
> **__check_cluster(client)**
> > Take a current EMR client, check clusters in state “WAITING” and “RUNNING”  and return available to run a spark job.
> 
> **__prepare_job_args(start_job_list: List[str])**
> **__prepare_job_options()**
> 
> > Prepares arguments of a spark job for adding to job flow.
> **define_job_flow_step(name: str, action_on_failure: str, hadoop_jar_step: str)**
> > Takes arguments describing  flow step for running spark job and returns a dictionary with step description which appends to Steps [] argument of Boto3 Emr client’s add_job_flow_steps() method.
> 
> **define_copy_file_step(name: str, action_on_failure: str)**
> > Takes arguments describing job flow steps for copying files on EMR cluster and returns a dictionary with step description which appends to Steps [] argument of Boto3 Emr client’s add_job_flow_steps() method.
> 
> **submit_job(run_async: bool = False, action_on_failure: str ='CANCEL_AND_WAIT')**. 
> > Takes bool arguments run_async for the running job asynchronously and action_on_failure to define cluster action on failing jobs, add job flow steps and return a python dictionary description of the job in case of asynchronously running or run __wait_for_job() method.
> 
> **__wait_for_job()** 
> > Waits for running job flow to complete. Returns current job description.
> 
> **_get_step_status(step_id: str)**
> > Checks status of current step in job flow. Returns state, step id and flag if step is done.
> 
> **get_job_state()** 
> > Returns current job flow step.
> 
> **_get_job()**
> > Returns current job description.
> 
> **cancel_job()**
> > Cancels current job.
> 
> **__upload_files_to_s3(files: List[str])**
> > Uploads files to S3 if necessary.
> 
> **upload_file_to_s3_job_path(file_path)** 
> > Uploads file to given s3 path.
> 
> **upload_script_to_s3_job_path(script: PyScript)** 
> > Upload PyScript script .
> 
> **__construct_path(name)** 
> > Construct path to file if file copies from local to s3.
> 
> **delete_s3_folder_source()**
> > Delete folder on s3.
> 
> **__file_path_to_s3_path(path)**
> > Constructs path to file on s3 where it uploads.
> 
> **job_description()**
> > Run __build_job_description() method.
> 
> **__build_job_description()**
> > Returns python dictionary with spark job description.
