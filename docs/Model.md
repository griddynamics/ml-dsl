### Class Model

**class com.griddynamics.dsl.ml.models.models.Model**

Present class Model.

**Properties**  
|property   |  description                                                                           
|-----------|-----------------------------------------------------------------------------------------|
| __files_root |  Path to folder with files for the Model instance.|
| __name   |  Name of the Model instance.|
| __version | Version of the Model instance.|
| __model_file | Name of file to save trained Model. |
| __model_uri | Cloud Storage path to trained Model. |
| __train_job_path | Path on cloud resource with train job artifacts. |
| __is_tuned | Boolean. Flag showing if the hyperparameter search was done. |
| __train_arguments | List of [Arguments](https://github.com/griddynamics/ml-dsl/blob/master/docs/Arguments.md) for training the Model. |
| __artifacts | List of Model [Artifacts](https://github.com/griddynamics/ml-dsl/blob/master/docs/Artifact.md). |
| __custom_predictor_path | Path to custom code of Model predictor. |
| __train_module | Name of train module. |
| __train_job_id | Job ID for train Model on [Cloud Platform](https://github.com/griddynamics/ml-dsl/blob/master/docs/Platform.md).|
