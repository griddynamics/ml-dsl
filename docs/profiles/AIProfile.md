### Class AIProfile(Profile)

**class com.griddynamics.dsl.ml.settings.profiles.AIProfile(bucket, cluster, region, job_prefix, root_path, project, ai_region, job_async, package_dst, scale_tier, package_name, runtime_version)**

Creates a profile with necessary information for training, deployment or test jobs.  

**Properties**  
Properties of [Profile](https://github.com/griddynamics/ml-dsl/blob/master/docs/profiles/Profile.md)  
|property          | type  | description                                                                           
|------------------|-------|-----------------------------------------------------------------------------------------|
| package_dst      | string| Path to folder where to set up the package with custom code.                            |
| package_name     | string| Name of package with custom code.                                                       |
| scale_tier       | string| The predefined configurations (the number and types of machines)  need for your training job. [List of available configurations for Google AIPlatform](https://cloud.google.com/ai-platform/training/docs/machine-types#scale_tiers) |
| runtime_version  | string| Runtime version to configure cloud resources to service your training and prediction requests. [List of available runtime versions for Google AIPlatform](https://cloud.google.com/ai-platform/training/docs/runtime-version-list) |


#### Example

> >     profile = AIProfile(bucket='test_bucket',
> >                         cluster='test_cluster', 
> >                         region='global', 
> >                         job_prefix='test_job', 
> >                         root_path='/home/test/scripts', 
> >                         project='test_project', 
> >                         ai_region='us-central1', 
> >                         job_async=False,
> >                         package_name='trainer', package_dst='packages',
> >                         scale_tier='BASIC', runtime_version='1.14')
