# Databricks notebook source
# MAGIC %md
# MAGIC * Unfortunately MLflow does not have an API method to read in a model as a Spark Bundle.
# MAGIC * So we have to manually construct the bundle URI and directly deserialize it with MLeap methods.
# MAGIC * https://databricks.aha.io/ideas/ideas/DB-I-477 - Add mleap.load_model() to MLflow Python API

# COMMAND ----------

from pyspark.ml import PipelineModel
from mleap.pyspark.spark_support import SimpleSparkSerializer

def load_model_as_spark_bundle(run, artifact_path):
    bundle_uri = f"file:{run.info.artifact_uri}/" + artifact_path
    bundle_uri = bundle_uri.replace("dbfs:","/dbfs")
    print("bundle_uri:",bundle_uri)
    return PipelineModel.deserializeFromBundle(bundle_uri)