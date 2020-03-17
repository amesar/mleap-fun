# Databricks notebook source
# MAGIC %md ## Read MLeap bundle as SparkBundle from MLflow

# COMMAND ----------

# MAGIC %md ### Setup

# COMMAND ----------

# MAGIC %run ../Common

# COMMAND ----------

# MAGIC %md ### Read data

# COMMAND ----------

data = spark.read.csv(data_path, header="true", inferSchema="true")
(trainingData, testData) = data.randomSplit([0.7, 0.3], 2019)

# COMMAND ----------

# MAGIC %md ### Get MLflow run

# COMMAND ----------

import mlflow
client = mlflow.tracking.MlflowClient()
experiment_id = client.get_experiment_by_name(experiment_name_python).experiment_id

# COMMAND ----------

run = client.search_runs(experiment_id, order_by=["attributes.start_time desc"], max_results=1)[0]
run

# COMMAND ----------

run.info.artifact_uri

# COMMAND ----------

# MAGIC %md ### Read MLeap bundle

# COMMAND ----------

# MAGIC %run ./MLeap_Utils

# COMMAND ----------

model = load_model_as_spark_bundle(run, "mleap-model/mleap/model")

# COMMAND ----------

# MAGIC %md ### Run predictions

# COMMAND ----------

predictions = model.transform(data)

# COMMAND ----------

display(predictions.select(colPrediction, colLabel, colFeatures))