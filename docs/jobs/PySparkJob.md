### Class PySparkJob(JSOEncoder)   
**class com.griddynamics.dsl.ml.jobs.pyspark_job.PySparkJob(JSONEncoder)**   
Describe a PySpark job.   
#### Properties
> **__job_file**
> > Path to main python file for spark job.
>
> **__py_files**
> > Python files for spark job.
>
> **__files**
> > Additional files for spark job.
>
> **__args**
> > Arguments for spark job. Example: {"path_to_save_results": "s3n://test_bucket/test_path"}
>
> **__properties**
> > Properties for spark job. Example: {'spark.executor.cores': '1',  'spark.executor.memory': '4G'}
>
> **__packages**
> > Packages for running jobs. Example: ["org.apache.hadoop:hadoop-aws:2.6.0"].
>
> **__jars**
> > JARs for spark job.
>
> **__archives**
> > Archives for spark job.
>
> **__logging**
> > Logging levels for spark job.
>
> **__files_root**
>
> **__task_script**
> > Task script for spark job.
>
> >**__run_async**
> > Boolean. Flag shows if is a streaming job.
>
> **__job_id**
> > ID of spark job.
>
> **__py_scripts**
> > [PyScript](https://github.com/griddynamics/ml-dsl/blob/master/docs/PyScript.md) instance for spark job.
>
> **__max_failures**
> > Quantity of failures for a spark job.
>
> **__labels**


#### Methods  
> **generate_run_script(py_script: [PyScript](https://github.com/griddynamics/ml-dsl/blob/master/docs/PyScript.md))**
> > Generate a [PyScript](https://github.com/griddynamics/ml-dsl/blob/master/docs/PyScript.md) instance for a job.
> 
> **from_profile(profile)**
> > Load spark job description from [Profile](https://github.com/griddynamics/ml-dsl/blob/master/docs/profiles/PySparkJobProfile.md).
