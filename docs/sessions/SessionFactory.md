### Class AbstractSessionFactory(ABC)   
**class com.griddynamics.dsl.ml.sessions.AbstractSessionFactory**   
Abstract class for Session Factory.  
#### Methods  
> **build_session(\*\*kwargs)**
> > Abstract method for building sessions.
> 
> **get_session(\*\*kwargs)**
> > Static method for getting a session.


### Class SessionFactory   
**class com.griddynamics.dsl.ml.sessions.SessionFactory**   
Return instance of SessionFactory based on [Platform](https://github.com/griddynamics/ml-dsl/blob/master/docs/Platform.md). 


### Class GCPSessionFactory(AbstractSessionFactory)  
**class com.griddynamics.dsl.ml.sessions.GCPSessionFactory**   
Abstract class for GCP Session Factory.   
#### Properties   
> **__composite_session**  
#### Methods  
> **get_session()**  
> > Get __composite_session.  
>
> **build_session(job_bucket, job_region, cluster, job_project_id, ml_region=None, project_local_root='jobs-root', project_env_root='', ml_project_id=None, ml_bucket=None)**  
> > Build session based on given input.  


### Class AWSSessionFactory(AbstractSessionFactory)  
**class com.griddynamics.dsl.ml.sessions.AWSSessionFactory**   
Abstract class for AWS Session Factory.   
#### Properties   
> **__composite_session**  
#### Methods  
> **get_session()**  
> > Get __composite_session.  
>
> **build_session(job_bucket, job_region, cluster, job_project_id, ml_region=None, project_local_root='jobs-root', project_env_root='', ml_project_id=None, ml_bucket=None)**  
> > Build session based on given input.  
