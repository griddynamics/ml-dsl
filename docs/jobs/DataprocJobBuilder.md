### Class DataprocJobBuilder   
**class com.griddynamics.dsl.ml.jobs.builder.DataprocJobBuilder**   
Build  a [PySparkJob](https://github.com/griddynamics/ml-dsl/blob/master/docs/jobs/PySparkJob.md) job to run on Cloud Platform.     
#### Methods  
> **files_root(files_root: str)**
> > Assign files for a job.
> 
> **task_script(name: str, py_scripts: dict)**
> > Assign [PyScript](https://github.com/griddynamics/ml-dsl/blob/master/docs/PyScript.md) task script to a job.
> 
> **arguments(arguments: [Arguments](https://github.com/griddynamics/ml-dsl/blob/master/docs/Arguments.md))**
> > Assign [Arguments](https://github.com/griddynamics/ml-dsl/blob/master/docs/Arguments.md) arguments to a job.
> 
> **py_script(name: str, py_scripts: dict)**
> > Assign [PyScript](https://github.com/griddynamics/ml-dsl/blob/master/docs/PyScript.md) task script to a job.
> 
> **reset()**
> > Assign new [PySparkJob](https://github.com/griddynamics/ml-dsl/blob/master/docs/jobs/PySparkJob.md).
> 
> **build_job(profile)**
> > Build a new [PySparkJob](https://github.com/griddynamics/ml-dsl/blob/master/docs/jobs/PySparkJob.md) job using information from [Profile](https://github.com/griddynamics/ml-dsl/blob/master/docs/profiles/PySparkJobProfile.md).
> 
> **job_file(val)**
> > Assign a job file to  a new [PySparkJob](https://github.com/griddynamics/ml-dsl/blob/master/docs/jobs/PySparkJob.md) job.
> 
> **run_async()**
> > Set run_async to True.
> 
> **job_id(val)**
> > Set job_id to the [PySparkJob](https://github.com/griddynamics/ml-dsl/blob/master/docs/jobs/PySparkJob.md).
> 
> **py_files(val)**
> > Set python files to the [PySparkJob](https://github.com/griddynamics/ml-dsl/blob/master/docs/jobs/PySparkJob.md) property py_files.
> 
> **file(val)**
> > Set files to the [PySparkJob](https://github.com/griddynamics/ml-dsl/blob/master/docs/jobs/PySparkJob.md) property files.
> 
> **max_failures(val)**
> > Set max_failures property to the [PySparkJob](https://github.com/griddynamics/ml-dsl/blob/master/docs/jobs/PySparkJob.md).
> 
> **logging(val)**
> > Set logging property of the [PySparkJob](https://github.com/griddynamics/ml-dsl/blob/master/docs/jobs/PySparkJob.md).
> 
> **jar(val)**
> > Set jars property of the [PySparkJob](https://github.com/griddynamics/ml-dsl/blob/master/docs/jobs/PySparkJob.md).
> 
> **property(key, val)**
> > Set properties of the [PySparkJob](https://github.com/griddynamics/ml-dsl/blob/master/docs/jobs/PySparkJob.md) as {key: val} dictionary.
> 
> **archives(val)**
> > Set archives property of the [PySparkJob](https://github.com/griddynamics/ml-dsl/blob/master/docs/jobs/PySparkJob.md).
> 
> **label(key, val)**
> > Set labels of the [PySparkJob](https://github.com/griddynamics/ml-dsl/blob/master/docs/jobs/PySparkJob.md) as {key: val} dictionary.
