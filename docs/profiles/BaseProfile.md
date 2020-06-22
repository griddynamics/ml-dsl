### Class BaseProfile

**class com.griddynamics.dsl.ml.settings.profiles.BaseProfile(bucket, cluster, region, job_prefix)**

Creates a profile with necessary information for a job.

**Class attributes**   
> _profiles={} 
> > Dictionary contains all available profiles   

**Properties**  
|property   | type  | description                                                                           
|-----------|-------|-----------------------------------------------------------------------------------------|
| bucket    | string| The Cloud Storage bucket to stage files in. Defaults to the cluster's configured bucket.|
| cluster   | string| Name of GKE cluster.                                                                    |
| region    | string| Cloud Resource region for running jobs.                                                 |
| job_prefix| string| Name of job. For example:  'demo_job_'                                                  |

#### Methods
> **set(name, profile)**
> > Static method. Add the instance of class Profile (profile) with name to class attribute _profiles. Example:  
> > 
> >     BaseProfile.set('TestProfile', profile) 

> **get(name)**
> > Static method. Get profile from class attribute _profiles with the name. Example:  
> > 
> >     BaseProfile.get('TestProfile')    

> **load_profile_data(file_path)**
> > Static method. Get profile data from json file.
