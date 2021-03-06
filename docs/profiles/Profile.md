### Class Profile(BaseProfile)

**class com.griddynamics.dsl.ml.settings.profiles.Profile(bucket, cluster, region, job_prefix, root_path, project, ai_region,  job_async)**

Creates a profile with necessary information for a job.

**Class attributes**   
> _profiles={} 
> > Dictionary contains all available profiles   

**Properties**  
Properties of [BaseProfile](https://github.com/griddynamics/ml-dsl/blob/master/docs/profiles/BaseProfile.md)  
|property   | type  | description                                                                           
|-----------|-------|-----------------------------------------------------------------------------------------|
| root_path | string| Path to folder with task scripts.                                                       |
| project   | string| Name of Cloud Storage Project.                                                          |
| ai_region | string| Cloud Resource region for ai jobs.                                                      |
| job_async | string| Return immediately, without waiting for the operation in progress to complete.          |


#### Example

> >     profile = Profile(bucket='test_bucket',
> >                       cluster='test_cluster', 
> >                       region='global',   
> >                       job_prefix='test_job', 
> >                       root_path='/home/test/scripts', 
> >                       project='test_project', 
> >                       ai_region='us-central1',
> >                       job_async=False)
