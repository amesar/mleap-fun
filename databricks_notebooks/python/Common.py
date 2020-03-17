# Databricks notebook source
# MAGIC %md Common Python code

# COMMAND ----------

import os
user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("user").get()
base_dir =  "/dbfs/tmp/" + user
work_dir = os.path.join(base_dir,"mleap_demo")
work_dir_python = os.path.join(work_dir,"python")
work_dir_scala = os.path.join(work_dir,"scala")
work_dir_dbfs = work_dir.replace("/dbfs","dbfs:")
data_path = os.path.join(work_dir_dbfs, "wine-quality.csv")

os.environ["WORK_DIR"] = work_dir
print("data_path:",data_path)
print("work_dir:",work_dir)

# COMMAND ----------

bundle_path_python = os.path.join(work_dir,"python/wine-bundle.zip")

# COMMAND ----------

experiment_name_python = f"/Users/{user}/MLeap_Sampler_Python"
experiment_name_scala = f"/Users/{user}/MLeap_Sampler_Scala"
print("experiment_name_python:",experiment_name_python)
print("experiment_name_scala: ",experiment_name_scala)

# COMMAND ----------

host_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("browserHostName").get()

def display_run_uri(experiment_id,run):
    uri = f"https://{host_name}/#mlflow/experiments/{experiment_id}/runs/{run.info.run_id}"
    displayHTML("""<b>Run URI:</b> <a href="{}">{}</a>""".format(uri,uri))

# COMMAND ----------

# MAGIC %md Common read methods

# COMMAND ----------

def createBundleWidget(name = "Bundle") :
  dbutils.widgets.dropdown(name,"Scala",["Scala","Python"])
  dbutils.widgets.get(name)

# COMMAND ----------

bundlePathPython = work_dir_python + "/wine-bundle.zip"
bundlePathSpark = work_dir_scala + "/wine-bundle.zip"

def getBundlePath(bundleName) : return bundlePathSpark if bundleName=="Scala" else bundlePathPython
def getBundleUri(bundleName) : return "jar:file:" + getBundlePath(bundleName)

# COMMAND ----------

experimentNameBase = f"/Users/{user}/tmp/MLeap_Sampler"
experimentNameScala = f"{experimentNameBase}_Scala"

def getExperimentNameForBundle(bundle) : return f"${experimentNameBase}_{bundle}"

# COMMAND ----------

colLabel = "quality"
colPrediction = "prediction"
colFeatures = "features"