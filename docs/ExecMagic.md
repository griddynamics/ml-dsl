### Class ExecMagic

#### %py_script
> Register cell content as a task script and save it as a python file with specified path and filename. Return object of the [PyScript](https://github.com/griddynamics/ml-dsl/blob/master/docs/PyScript.md) class.  
> |argument  | description|
> |----------|--------------------------|
> |-p, --path| Path to save task script.|
> |-n, --name| Name of script file.|
> |-e, --exec| Define if a task script is being executed at once running the cell.|
> |-o, --output_path| Output path for task script results.|
> 
> ##### Example
> 
> >     In [1]: %py_script -e -n test_job.py -p test/scripts -o results

#### %py_script_open
> Load the content of the python script and register it as a task script. Return object of the [PyScript](https://github.com/griddynamics/ml-dsl/blob/master/docs/PyScript.md) class.  
> |argument  | description|
> |----------|--------------------------|
> |-p, --path| Path to script’s folder.|
> |-n, --name| Name of script file.|
> |-o, --output_path| Output path for task script results.|
> 
> ##### Example
> 
> >     In [1]: %py_script_open -n test_job.py -p test/scripts -o results

#### %py_load
> Loads content of python script to the cell. 
> ##### Example
> 
> >     In [1]: %py_load test/scripts/test_job.py

#### %py_data
> Start a data processing job with specified parameters. Return information about task job status as well as links to job on specified cloud platform and task output path.  
> | argument           | description  |
> |--------------------|------------------------------------------|
> | -n, --name         | Name of script file. |
> | -pm, --platform    | Cloud [Platform](https://github.com/griddynamics/ml-dsl/blob/master/docs/Platform.md)to run the specified task.|
> | -p, --profile      | Name of [Profile](https://github.com/griddynamics/ml-dsl/blob/master/docs/profiles/PySparkJobProfile.md) which describes the identifying information, config of a cluster of Compute Engine instances, task’s  and additional information.|
> | -o, --output_path  | Output path for task script results. Example: gs://cluster/mldsl_example/prepared_data (Google Cloud Storage resource)|
> | optional arguments | Any [Arguments](https://github.com/griddynamics/ml-dsl/blob/master/docs/Arguments.md) described for your task. It’s recommended to use the functionality of [Profile](https://github.com/griddynamics/ml-dsl/blob/master/docs/profiles/PySparkJobProfile.md) class to describe the information.|
> 
> ##### Example
> 
> >     In [1]: platform = Platform.AWS
> >     In [2]: PROJECT_ID = 'test_job'
> >     In [3]: profile = Profile(...)
> >     	   ...
> >     	  Profile.set(PROJECT_ID, profile)
> >     In [4]: %py_data -n test_job.py -p $PROJECT_ID -pm $platform -o $output_path

#### %py_train
> Execute a training  job with specified parameters.  
> | argument           | description  |
> |--------------------|------------------------------------------|
> | -n, --name         | Name of script file. |
> | -s, --package_src  | Package src directory. |
> | -pm, --platform    | Cloud [Platform](https://github.com/griddynamics/ml-dsl/blob/master/docs/Platform.md) to run the specified task.|
> | -p, --profile      | Name of [Profile](https://github.com/griddynamics/ml-dsl/blob/master/docs/profiles/AIProfile.md) which describes the identifying information, config of a cluster of Compute Engine instances, task’s  and additional information.|
> | -o, --output_path  | Output path for task script results. Example: gs://cluster/mldsl_example/models (Google Cloud Storage resource)|
> | optional arguments | Any [Arguments](https://github.com/griddynamics/ml-dsl/blob/master/docs/Arguments.md) described for your task. It’s recommended to use the functionality of [Profile](https://github.com/griddynamics/ml-dsl/blob/master/docs/profiles/AIProfile.md) class to describe the information.|
> 
> ##### Example
> 
> >     In [1]: platform = Platform.GCP
> >     In [2]: PROJECT_ID = 'demo_job'
> >     In [3]: ai_profile = AIProfile(...)
> >     	   ...
> >     	  Profile.set(PROJECT_ID, ai_profile)
> >     In [4]: %py_train -n ai_model.py -s demo/train -p $PROJECT_ID  -pm $platform -o path_to_save_model\
> >                       --train_path gs://train/demo_job_012345/train_data

#### %py_deploy
> Deploy your trained model on AI Platform.  
> | argument           | description  |
> |--------------------|------------------------------------------|
> | -n, --name         | Name of script file. |
> | -s, --package_src  | Package src directory. |
> | -pm, --platform    | Cloud Cloud [Platform](https://github.com/griddynamics/ml-dsl/blob/master/docs/Platform.md) to run the specified task.|
> | -p, --profile      | Name of [Profile](https://github.com/griddynamics/ml-dsl/blob/master/docs/profiles/DeployAIProfile.md) which describes the identifying information, config of a cluster of Compute Engine instances, task’s  and additional information.|
> 
> ##### Example
> 
> >     In [1]: platform = Platform.GCP
> >     In [2]: PROJECT_ID = 'demo_job'
> >     In [3]: ai_profile = DeployAIProfile(...)
> >     	   ...
> >     	  Profile.set(PROJECT_ID, ai_profile)
> >     In [4]: %py_deploy -n mldsl_demo -p $PROJECT_ID -s demo/deploy  -pm $platform

#### %py_test
> Get online predictions from model resource for test data. Return your predictions in the response and paste to the next cell. Currently only Google Cloud Platform supported.
> | argument           | description  |
> |--------------------|------------------------------------------|
> | -t, --test         | Test data to get online predictions. |
> | -pm, --platform    | Cloud Cloud [Platform](https://github.com/griddynamics/ml-dsl/blob/master/docs/Platform.md) to run the specified task.|
> | -p, --profile      | Name of [Profile](https://github.com/griddynamics/ml-dsl/blob/master/docs/profiles/DeployAIProfile.md) which describes the identifying information, config of a cluster of Compute Engine instances, task’s  and additional information.|
> 
> ##### Example
> 
> >     In [1]: platform = Platform.GCP
> >     In [2]: PROJECT_ID = 'demo_job'
> >     In [3]: ai_profile = DeployAIProfile(...)
> >     	   ...
> >     	  Profile.set(PROJECT_ID, ai_profile)
> >     In [4]: %py_test -p $PROJECT_ID  -pm $platform -t $test

#### %logging
> Get logs of the specified Dataproc job and print them out. Currently only Google Cloud Platform supported.
> | argument           | description  |
> |--------------------|------------------------------------------|
> | -p, --project_id         | Project ID on Google Cloud Platform. |
> | -f, --filter    | Conditions to filter specific values of indexed fields, like the log entry's name, resource type, and resource labels.|
> | -r, --order_by      | One of ASC, DESC or None.|
> | -s, --page_size      | The maximum number of entries in each page of results.|
> 
> ##### Example
> 
> >     In [1]: project = test_project
> >     In [2]: filter = 'jsonPayload.application:application_123'
> >     In [3]: %logging -p $project -f $filter

#### %job_upgrade
> Implement changes for jobs. Start a new version of the job, wait for necessary validation (logs checking) and stop one of the job's version based on this checking: old one if validation was successful, new if the conditions were not met.  
> | argument           | description  |
> |--------------------|------------------------------------------|
> | -n, --name         | Name of script file. |
> | -pm, --platform    | Cloud Cloud [Platform](https://github.com/griddynamics/ml-dsl/blob/master/docs/Platform.md) to run the specified task.|
> | -p, --profile      | Name of [Profile](https://github.com/griddynamics/ml-dsl/blob/master/docs/profiles/PySparkJobProfile.md) which describes the identifying information, config of a cluster of Compute Engine instances, task’s  and additional information.|
> | -o, --output_path  | Output path for task script results. Example: gs://cluster/mldsl_example/prepared_data (Google Cloud Storage resource)|
> | --old_job_id | Name of job’s previous version to upgrade.|
> | -v, --validator | Name of custom validator class.|
> | -vp, --validator_path | Path to script with validator class.|
> | optional arguments | Any [Arguments](https://github.com/griddynamics/ml-dsl/blob/master/docs/Arguments.md) described for your task. It’s recommended to use the functionality of [Profile](https://github.com/griddynamics/ml-dsl/blob/master/docs/profiles/PySparkJobProfile.md) class to describe the information.|
> 
> ##### Example
> 
> >     In [1]: platform = Platform.GCP
> >     In [2]: PROJECT_ID = 'test_upgrade_job'
> >     In [3]: profile = Profile(...)
> >     	   ...
> >     	  Profile.set(PROJECT_ID, profile)
> >     In [4]: %job_upgrade -n test_upgrade_job.py -p $PROJECT_ID -pm $platform -o gs://ai4ops/mldsl/data\
> >                          --old_job_id old_job_123 -v MyValidator -vp  /.mldsl/my_validator.py

