### Class ModelBuilder

**class com.griddynamics.dsl.ml.models.models.ModelBuilder**

Present class to build [Model](https://github.com/griddynamics/ml-dsl/blob/master/docs/Model.md) description.

#### Properties  
|property   | type  | description                                                                           
|-----------|-------|-----------------------------------------------------------------------------------------|
| __artifacts    | List()| [Artifacts](https://github.com/griddynamics/ml-dsl/blob/master/docs/Artifact.md) of ModelBuilder instance.|
| __packages   | List()| Packages of ModelBuilder instance.|
| __train_arguments | List()| [Arguments](https://github.com/griddynamics/ml-dsl/blob/master/docs/Arguments.md) of ModelBuilder instance.|
| __model| [Model](https://github.com/griddynamics/ml-dsl/blob/master/docs/Model.md)| Model of ModelBuilder instance. |


#### Methods  
> **_reset()** 
> > Initialize new Model instance.  
> 
> **files_root(files_rooot)** 
> > Set files_root Model argument. 
> 
> **train_job_id(train_job_id)** 
> > Set train_job_id argument of Model.
> 
> **train_image_uri(train_image_uri)** 
> > 
>
> **model_file(model_file)**
> > Set model_file argument of Model.
>
> **model_uri(model_uri)**
> > Set model_uri argument of Model.
>
> **custom_predictor_path(custom_predictor_path)**
> > Set custom_predictor_path argument of Model.
>
> **artifact(artifact: [Artifact](https://github.com/griddynamics/ml-dsl/blob/master/docs/Artifact.md))**
> > Append artifact to the __artifacts property of it’s instance.
>
> **artifacts(artifacts: \[[Artifact](https://github.com/griddynamics/ml-dsl/blob/master/docs/Artifact.md)\])**
> > Set the artifacts to the __artifacts property of it’s instance.
>
> **package(package: [Artifact](https://github.com/griddynamics/ml-dsl/blob/master/docs/Artifact.md))**
> > Append the package to the __packages property of it’s instance.
>
> **packages(self, packages: [])**
> > Set the packages to the __packages property of it’s instance.
>
> **train_argument(train_argument)**
> > Append the train_argument to the __train_arguments property of it’s instance.
>
> **train_arguments(train_arguments: [Arguments](https://github.com/griddynamics/ml-dsl/blob/master/docs/Arguments.md))**
> > Extend the __train_arguments property of it’s instance with given train_arguments.
>
> **is_tuning(is_tuning)**
> > Set flag if the Model is tuning.
>
> **version(version)**
> > Set version of the Model.
>
> **name(name)**
> > Set name of the Model.
>
> **build()**
> > Build and return a new Model instance. 

