# Databricks notebook source

courseType = "sp"

displayHTML("Preparing the Python environment...")

# COMMAND ----------

# MAGIC %run ./Class-Utility-Methods

# COMMAND ----------

# MAGIC %run ./Utility-Methods

# COMMAND ----------

moduleName = getModuleName()
lessonName = getLessonName()
username = getUsername()
userhome = getUserhome()
workingDir = getWorkingDir(courseType)
databaseName = createUserDatabase(courseType, username, moduleName, lessonName)

displayHTML("""
Defining custom variables for this lesson...
<ul>
  <li><span style="color: #339966; font-weight:bold">moduleName</span>: <span style="font-style:italic; font-weight:bold">{}</span></li>
  <li><span style="color: #339966; font-weight:bold">lessonName</span>: <span style="font-style:italic; font-weight:bold">{}</span></li>
  <li><span style="color: #339966; font-weight:bold">username</span>: <span style="font-style:italic; font-weight:bold">{}</span></li>
  <li><span style="color: #339966; font-weight:bold">userhome</span>: <span style="font-style:italic; font-weight:bold">{}</span></li>
  <li><span style="color: #339966; font-weight:bold">workingDir</span>: <span style="font-style:italic; font-weight:bold">{}</span></li>
</ul>

Created a lesson-specific database:
<ul>
  <li><span style="color: #339966; font-weight:bold">databaseName</span>: <span style="font-style:italic; font-weight:bold">{}</span></li>
</ul>
""".format(moduleName,lessonName,username,userhome,workingDir,databaseName)
)

# COMMAND ----------

# MAGIC %run ./Assertion-Utils

# COMMAND ----------

# MAGIC %run ./Dummy-Data-Generator

# COMMAND ----------

# MAGIC %run ./Dataset-Mounts

# COMMAND ----------

# This script sets up MLflow and handles the case that 
# it is executed by Databricks' automated testing server

def mlflowAttached():
  try:
    import mlflow
    return True
  except ImportError:
    return False

  
if mlflowAttached():
  import os
  import mlflow
  from mlflow.tracking import MlflowClient
  from databricks_cli.configure.provider import get_config
  from mlflow.exceptions import RestException
  
  os.environ['DATABRICKS_HOST'] = get_config().host
  os.environ['DATABRICKS_TOKEN'] = get_config().token
  
  if getTag("notebookId"):
    os.environ["MLFLOW_AUTODETECT_EXPERIMENT_ID"] = 'true'
  else:
    # Handles notebooks run by test server (executed as run)
    _name = "/Shared/BuildExperiment"
    
    try:
      client = MlflowClient()
      experiment = client.get_experiment_by_name("/Shared/BuildExperiment")

      if experiment:
        client.delete_experiment(experiment.experiment_id) # Delete past runs if possible
        
      mlflow.create_experiment(_name)
    except RestException: # experiment already exists
      pass
    
    os.environ['MLFLOW_EXPERIMENT_NAME'] = _name
  
  # Silence YAML deprecation issue https://github.com/yaml/pyyaml/wiki/PyYAML-yaml.load(input)-Deprecation
  os.environ["PYTHONWARNINGS"] = 'ignore::yaml.YAMLLoadWarning' 
  
None # suppress output

# COMMAND ----------

assertDbrVersion(6, 3)
None # Suppress output

# COMMAND ----------

classroomCleanup(daLogger, courseType, username, moduleName, lessonName, False)
None # Suppress output

