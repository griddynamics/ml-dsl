### Class Helper   
**class com.griddynamics.dsl.ml.helpers.Helper**   
Class Helper for organizing work with files, cloud storages etc.         
#### Methods   
> **build_package(name, script_names: [], script_args=None, version="1.0", requires=None)**
> > Static method. Building additional modules into a Python installation.
>
> **get_file(file)**
> > Static method. Get file content.
>
> **get_file_name(path)**
> > Static method. Get file name from full path.
>
> **get_setup_params(setup_path)**
> > Static method. Get parameters to set up the package.
>
> **build_package_name_from_params(kwargs, extension)**
> > Static method. Build package.
>
> **construct_path(filename: str, base_path, platform='GCP')**
> > Static method. Construct path to file based on platform (‘GCP’ and ‘AWS’).
