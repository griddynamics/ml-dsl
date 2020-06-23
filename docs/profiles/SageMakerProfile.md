### Class SageMakerProfile([BaseProfile](https://github.com/griddynamics/ml-dsl/blob/master/docs/profiles/BaseProfile.md))

**class com.griddynamics.dsl.ml.settings.profiles.SageMakerProfile(bucket, cluster, region, job_prefix, container, root_path, framework_version, instance_type,  instance_count, endpoint_name, model_data, py_version)**

Creates a profile with necessary information for training, deployment or test jobs on SageMaker. Inherited from [BaseProfile](https://github.com/griddynamics/ml-dsl/blob/master/docs/profiles/BaseProfile.md). 

**Properties**  
Properties of [BaseProfile](https://github.com/griddynamics/ml-dsl/blob/master/docs/profiles/BaseProfile.md)  
|property   | type        | description                                                                           
|-----------|-------------|---------------------------|
| container  | SageMakerEstimator| Sagemaker estimator. For example sagemaker.pytorch.PyTorch|
| root_path     | string| Path to folder with task scripts.|
| framework_version      | string| Version of container. For example sagemaker.pytorch.PyTorch framework_version ‘1.4.0’ |
| instance_type |string| Amazon SageMaker ML Instance Types. [List of available instance types](https://aws.amazon.com/sagemaker/pricing/instance-types)|
| instance_count | Int| Number of Amazon SageMaker ML Instances.|
| endpoint_name | string| Name of endpoint where a model was served. |
| model_data | string| Path to trained and saved model on s3. |
| py_version | Dict()| Version of python used. |

#### Example

> >     profile = SageMakerProfile(bucket='test-bucket', cluster='test-cluster',
> >                                region='us-east-1', job_prefix='mldsl_test',
> >                                container=PyTorchModel, framework_version='1.4.0',         
> >                                instance_type='ml.m4.xlarge', instance_count=2)
> >     profile.model_data = 's3://sagemaker-.../pytorch-model/model.tar.gz'
