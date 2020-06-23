### Class PyScript

**class com.griddynamics.dsl.ml.PyScript**

Python script model.

**Class attributes**   
> **__script: str** 
> > Python script for execution. Must contains __main__ method.  
> 
> **__package: str** 
> > Package of the script (optional). 
> 
> **__name: str** 
> > Name of the script (optional).
> 
> **__class_name: str** 
> > Name of class from the script which is inherited from Task.
>
> **__state: class: ScriptState**
> > State of script (optional).

**Properties**  
|property   | type  | description                                                                           
|-----------|-------|-----------------------------------------------------------------------------------------|
| script    | string| Python script for execution. Must contains __main__ method.|
| name   | string| Name of the script (optional). Default value is ‘default.py’|
| __class_name | string| Name of class from the script which is inherited from Task. Default value is None.|
| __state| class: ScriptState| State of script (optional). |
