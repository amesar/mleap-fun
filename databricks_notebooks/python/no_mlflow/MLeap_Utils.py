# Databricks notebook source
# MAGIC %md Encapsulate MLeap-specific logic in this method.

# COMMAND ----------

from pyspark.ml import PipelineModel
from mleap.pyspark.spark_support import SimpleSparkSerializer

def read_model_as_spark_bundle(bundle_uri):
    return PipelineModel.deserializeFromBundle(bundle_uri)