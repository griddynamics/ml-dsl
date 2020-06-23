### Class CompositeSession

**class com.griddynamics.dsl.ml.sessions.CompositeSession**

Class for interaction with SageMaker AWS.

#### Properties
> **__ml_session**
>
> **__job_session**
> 
> **__local_project_root**
> 
> **__env_project_root**

#### Methods  
> **get_job_session()**
> > Abstract method for getting a job session.
> 
> **get_ml_session()**
> > Abstract method for getting a ML session.
> 
> **__build(job_bucket, job_region, cluster, job_project_id, platform, job_path='jobs-root', ml_region=None, ml_project_id=None, ml_bucket=None)**
> > Building composite session. platform - one of ‘GCP’ or ‘AWS’.
