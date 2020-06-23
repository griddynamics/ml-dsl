### Class Session

**class com.griddynamics.dsl.ml.sessions.Session**

Build a session based on given settings for a given [Platform](https://github.com/griddynamics/ml-dsl/blob/master/docs/Platform.md).

#### Properties
> **__bucket**
> > String. Bucket on Cloud Platform.
>
> **__jobs_path**
> > String. Job’s path on Cloud Platform.
>
> **__zone**
> > String. Zone on Cloud Platform if applicable.
>
> **__cluster**
> > String. Cluster on Cloud Platform.
>
> **__project_id**
> > String. Project Id on Cloud Platform if applicable.
>
> **__region**
> > String. Region on Cloud Platform
>
> **__cluster_uuid**
> > String. Id of cluster on Cloud Platform if applicable
> 
> **__platform
> > String. Cloud Platform abbreviation. One of ‘AWS’ or ‘GCP’.

#### Methods  
> **get_region_from_zone(zone)**
> > Build name of Cloud Platform region based on zone if applicable.
