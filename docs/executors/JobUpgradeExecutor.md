### JobUpgradeExecutor(DataprocExecutor)

**class class com.griddynamics.dsl.ml.executors.executors.JobUpgradeExecutor**
Class for upgrading jobs on Google Dataproc. Inherited from [DataprocExecutor](https://github.com/griddynamics/ml-dsl/blob/master/docs/executors/DataprocExecutor.md).

#### Properties

|property   | description                                                                           
|-----------|---------------------------------------------|
| __old_job_id   | ID of Google Dataproc job to upgrade.|

#### Methods  
> **get_old_job_state()**
> > Get a state of old (upgraded) job on Google Dataproc.
> 
> **cancel_old_job()**
> > Cancel an old (upgraded) job on Google Dataproc.
> 
> **get_old_job()**
> > Get an old (upgraded) job on Google Dataproc.
> 
> **submit_upgrade_job(validator, validator_path, run_async=True)**
> > Running an upgrade  job procedure on Google Dataproc. Submitting a new job, checking the job based on validator instance of class inherited from Validator and canceling old or new job (based on validator running). Validator - name of custom Validator class. validator_path - path to script with validator class. run_async - set True if return result of streaming task.
