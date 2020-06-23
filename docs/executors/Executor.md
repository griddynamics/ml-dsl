### Class Executor(ABC)

**class com.griddynamics.dsl.ml.executors.executors.Executor**

Abstract class Executor.


**Methods**   
> **submit_job(job: [PySparkJob](https://github.com/griddynamics/ml-dsl/blob/master/docs/jobs/PySparkJob.md), session: [Session](https://github.com/griddynamics/ml-dsl/blob/master/docs/sessions/Session.md))** 
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
