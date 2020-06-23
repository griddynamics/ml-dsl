### Class AIPlatformJobExecutor

**class com.griddynamics.dsl.ml.executors.executors.AIPLatformJobExecutor**

Class for working with Google AIPlatform.


#### Properties

|property   | description                                                                           
|-----------|---------------------------------------------|
| session   | [Session](https://github.com/griddynamics/ml-dsl/blob/master/docs/sessions/Session.md) of a job.|
| ai_job   | AI job on AIPLatform.|
| project_id    | Project ID of an AI job.|
| wait_delay| Delay between attempts to run an AI job.|
| wait_tries| Number of  attempts to run an AI job.|

#### Methods
> **package()**
> > Setting up a package with scripts of the model.  
> 
> **upload_package()**
> > Uploading a model package to the directory on GCS.   
> 
> **submit_train_job()**
> > Submitting a train job on AIPlatform.
> 
> **submit_batch_prediction_job()**
> > Getting batch predictions of the model on AIPlatform.
> 
> **cancel_job(session: [Session](https://github.com/griddynamics/ml-dsl/blob/master/docs/sessions/Session.md), job_name=None)**
> > Static method. Cancelling a job on AIPlatform.
> 
> **create_model(model_name, online_prediction_logging=True)**
> > Creating a model on AIPlatform.
> 
> **submit_deploy_model_job(version_name, train_job_id=None, objective_value_is_maximum_needed=False, create_new_model=False)**
> > Deploy a trained model on AIPlatform. Returns result of deployment attempt.
> 
> **submit_prediction_job(predictions)**
> > Getting prediction of deployed model on AIPlatform for “predictions” json.
> 
> **get_job(session: [Session](https://github.com/griddynamics/ml-dsl/blob/master/docs/sessions/Session.md), full_job_name: str)**
> > Getting a job on AIPlatform.
> 
> **get_version(session: [Session](https://github.com/griddynamics/ml-dsl/blob/master/docs/sessions/Session.md), version_full_name: str)**
> > Getting a specific version of a job on AIPlatform.
> 
> **get_best_hp_tuning_result(session: [CompositeSession](https://github.com/griddynamics/ml-dsl/blob/master/docs/sessions/CompositeSession.md), job_name, objective_value_is_maximum_needed=False, debug=False)**
> > Returns the best result of the hyperparameter optimization process.
> 
> **set_and_get_best_model_path(best_trial, bucket, train_job_dir)**
> > Returns path with the best model.
> 
> **__wait_for_job(get_funk, name, wait_tries=5, delay=5, cancel_funk=None, \*stop_statuses)**
> > Waiting for a job is done on AIPlatform.
