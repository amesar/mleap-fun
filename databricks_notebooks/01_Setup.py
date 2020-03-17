# Databricks notebook source
# MAGIC %md ## Setup
# MAGIC * Create a working directory `/dbfs/$USER_NAME/tmp/mleap_demo`.
# MAGIC   * For example: `/dbfs/john.doe@acme.com/tmp/mleap_demo`.
# MAGIC * Download data to `/dbfs/$USER_NAME/tmp/mleap_demo/wine-quality.csv`

# COMMAND ----------

dbutils.widgets.dropdown("Delete working directory","yes",["yes","no"])
delete_dir = dbutils.widgets.get("Delete working directory") == "yes"
delete_dir

# COMMAND ----------

# MAGIC %run ./python/Common

# COMMAND ----------

import os, shutil
if delete_dir:
    print("Deleting ",work_dir)
    shutil.rmtree(work_dir)
os.makedirs(work_dir, exist_ok=True)
os.makedirs(os.path.join(work_dir,"python"), exist_ok=True)
os.makedirs(os.path.join(work_dir,"scala"), exist_ok=True)
work_dir

# COMMAND ----------

import requests

def download_file(data_uri, data_path):
    if os.path.exists(data_path):
        print(f"File {data_path} already exists")
    else:
        print(f"Downloading {data_uri} to {data_path}")
        rsp = requests.get(data_uri)
        with open(data_path, 'w') as f:
            f.write(requests.get(data_uri).text)
            
data_path = os.path.join(work_dir,"wine-quality.csv")
data_uri = "https://raw.githubusercontent.com/mlflow/mlflow/master/examples/sklearn_elasticnet_wine/wine-quality.csv"
download_file(data_uri, data_path)

# COMMAND ----------

# MAGIC %md ### MLflow Setup

# COMMAND ----------

import mlflow
client = mlflow.tracking.MlflowClient()

# COMMAND ----------

def deleteRuns(experiment_name):
  print("Experiment:",experiment_name)
  experiment = client.get_experiment_by_name(experiment_name)
  if experiment is not None:
    runs = client.search_runs(experiment.experiment_id,"")
    print("Experiments:",len(runs))
    for run in runs:
        client.delete_run(run.info.run_id)

# COMMAND ----------

deleteRuns(experiment_name_python)

# COMMAND ----------

deleteRuns(experiment_name_scala)

# COMMAND ----------

# MAGIC %md ### Check files

# COMMAND ----------

# MAGIC %sh echo $WORK_DIR

# COMMAND ----------

# MAGIC %sh ls -l $WORK_DIR

# COMMAND ----------

# MAGIC %sh ls -l $WORK_DIR/python

# COMMAND ----------

# MAGIC %sh ls -l $WORK_DIR/scala