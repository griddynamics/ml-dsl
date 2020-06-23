### Class SageMakerExecutor

**class com.griddynamics.dsl.ml.executors.executors.SageMakerExecutor**

Class for interaction with SageMaker AWS.

#### Properties

|property   | description                                                                           
|-----------|---------------------------------------------|
| __session   | Current working SageMaker session.|
| __bucket   | Default bucket for current SageMaker session.|
| __role    | Execution role for current SageMaker session.|
| __container| SageMaker estimator to train, deploy or predict. It’s recommended to use the functionality of [Profile](https://github.com/griddynamics/ml-dsl/blob/master/docs/profiles/SageMakerProfile.md) class to describe the information.|
| __instance_count| Number of instances for SageMaker estimator to train, deploy or predict. It’s recommended to use the functionality of [Profile](https://github.com/griddynamics/ml-dsl/blob/master/docs/profiles/SageMakerProfile.md) class to describe the information.|
| __instance_type| Type of instances for SageMaker estimator to train, deploy or predict. Example: “ml.m4.xlarge”. It’s recommended to use the functionality of [Profile](https://github.com/griddynamics/ml-dsl/blob/master/docs/profiles/SageMakerProfile.md) class to describe the information.|
| __framework_version| Framework version for SageMaker estimator to train, deploy or predict. Example: “1.4.0” for sagemaker PyTorch estimator. It’s recommended to use the functionality of [Profile](https://github.com/griddynamics/ml-dsl/blob/master/docs/profiles/SageMakerProfile.md) class to describe the information.|
| __py_version| Python version for SageMaker container. It’s recommended to use the functionality of [Profile](https://github.com/griddynamics/ml-dsl/blob/master/docs/profiles/SageMakerProfile.md) class to describe the information.|
| __model_data| Path to saved model on s3. It’s recommended to use the functionality of [Profile](https://github.com/griddynamics/ml-dsl/blob/master/docs/profiles/SageMakerProfile.md) class to describe the information.|
| __endpoint_name| Name of SageMaker endpoint. For sake of concision it’s recommended to use the functionality of [Profile](https://github.com/griddynamics/ml-dsl/blob/master/docs/profiles/SageMakerProfile.md) class to describe the information.|
| __arguments| Dictionary of arguments for a training job.|

#### Methods  
> **\_\_init\_\_(self, session: CompositeSession, profile, mode: str, py_script_name: str, args: dict)**
> > mode
> > Define which type of SageMaker estimator should be called:  
> > “train” - estimator to fit model;  
> > “deploy” - estimator to deploy already trained model;  
> > “predict” - estimator to predict.  
> 
> **submit_train_job()**
> > Starts fit method of given sagemaker container. Returns python dictionary with information about fitting.
> 
> **submit_deploy_model_job()**
> > Starts deployment of model with given sagemaker container. Returns python dictionary with information about deployment.

