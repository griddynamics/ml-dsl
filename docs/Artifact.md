### Class Artifact

**class com.griddynamics.dsl.ml.settings.artifacts.Artifact**

Present artifacts of the Model. Artifacts are files in any format. For example trained models (a pickled scikit-learn model), data files etc.

**Properties**   
> __file_name: str
> > Name of Artifact file.
>
> __path
> > Path to Artifact.


#### Methods
> **set_args(**kwargs)**
> > Updates the Argument dictionary with the elements from the another dictionary object or from an iterable of key/value pairs. The method adds element(s) to the dictionary if the key is not in the dictionary. If the key is in the dictionary, it updates the key with the new value.   
>
> **set_arg(key, value)**
> > Update one argument ‘key’ of Argument dictionary with given ‘value’.  
>
> **get_arg(key: str)**
> > Get argument from Argument dictionary by given key.
>
> **get()**
> > Return list of arguments from Argument dictionary.
