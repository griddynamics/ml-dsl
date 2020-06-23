### Class Executor(ABC)

**class com.griddynamics.dsl.ml.executors.executors.Executor**

Abstract class Executor.


**Methods**   
> **submit_job(job: PySparkJob, session: Session)** 
> > Abstract method for submitting a job.  
> 
> **get_job()** 
> > Abstract method for getting a job. 
> 
> **cancel_job()** 
> > Abstract method for cancelling a job.
> 
> **_download_output()** 
> > Abstract method for downloading output of a job.
>
> **get_job_state()**
> > Abstract method for getting a job state.
