### Class DeployAIProfile(AIProfile)

**class com.griddynamics.dsl.ml.settings.profiles.DeployAIProfile(bucket, cluster, region, job_prefix, root_path, project, ai_region, job_async, package_dst, scale_tier, package_name, runtime_version, python_version, use_cloud_engine_credentials=False, arguments={}, model_name='model', version_name='v1', is_new_model='True', artifacts=[], custom_code=None, path_to_saved_model='./')**

Creates a profile with necessary information for training, deployment or test jobs.  

**Properties**  
Properties of [AIProfile](https://github.com/griddynamics/ml-dsl/blob/master/docs/profiles/AIProfile.md)  
|property          | type  | description                                                                           
|------------------|-------|-----------------------------------------------------------------------------------------|
| model_name      | string| Name of model for deployment. |
| version_name     | string| Version of the model name. |
| is_new_model       | bool| Set to True if new model is deploying. |
| artifacts  | List() | List of paths to model artifacts|
| custom_code  | string | Paths to custom code for deployment|
| path_to_saved_model  | string | Path to model for deployment|


#### Example

> >     profile = DeployAIProfile(bucket='test_bucket',
> >                               cluster='test_cluster', 
> >                               region='global', 
> >                               job_prefix='test_job', 
> >                               root_path='/home/test/scripts', 
> >                               project='test_project', 
> >                               ai_region='us-central1', 
> >                               job_async=False,
> >                               package_name='trainer', package_dst='packages',
> >                               scale_tier='BASIC', runtime_version='1.14')
> >     profile.model_name = 'mldsl_demo'
> >     profile.version_name = 'v1'
> >     profile.is_new_model = True
> >     profile.path_to_saved_model = 'gs://bucket/models/my_model/'
