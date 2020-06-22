### Class PySparkJobProfile(Profile)

**class com.griddynamics.dsl.ml.settings.profiles.PySparkJobProfile(bucket, cluster, region, job_prefix, root_path, project, ai_region, job_async)**

Creates a profile with necessary information for pyspark jobs.  

**Properties**  
Properties of [Profile](https://github.com/griddynamics/ml-dsl/blob/master/docs/profiles/Profile.md)  
|property   | type        | description                                                                           
|-----------|-------------|---------------------------|
| py_files  | List(string)| List of python scripts for the job.|
| files     | List(string)| List of additional files for the job.|
| jars      | List(string)| List of jars for the job. |
| properties | Dict()| Dictionary of job properties. |
| args | Dict()| Dictionary of arguments for the job.|
| archives | List(string)| List of archives for the job. |
| packages | List(string)| List of packages for the job. |
| logging | Dict()| Dictionary of per-package log levels for the driver. |
| max_failures | Int| Maximum quantity job to fail. |


#### Example

> >     logging={"com.example":4,"root":4, "com.google.cloud.pyspark":4}
> >     py_files = ["./test_utils.py", "./test_job.py"]
> >     JARS = ["gs://test_bucket/resources/mysql-connector-java-8.0.16.jar"]
> >     properties = {"spark.executor.cores":"1", "spark.executor.memory":"4G"}
> >     profile = PySparkJobProfile(bucket='bucket',
> >                                 cluster='cluster',
> >                                 region='global',
> >                                 job_prefix='demo_job',
> >                                 root_path='~/demo/scripts',
> >                                 project='project',
> >                                 ai_region='us-central1',
> >                                 job_async=True,logging=logging)
> >     pysparkjob_profile.py_files = py_files
> >     pysparkjob_profile.jars=JARS
> >     pysparkjob_profile.properties=properties
> >     pysparkjob_profile.max_failures = 3
> >     pysparkjob_profile.args = {'--test_table': TEST_TABLE}

