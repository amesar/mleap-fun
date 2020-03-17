# Databricks notebook source
# MAGIC %md ## Read MLeap bundle as SparkBundle from filesystem

# COMMAND ----------

# MAGIC %md ### Setup

# COMMAND ----------

# MAGIC %run ../Common

# COMMAND ----------

bundle_name = createBundleWidget()
bundle_uri = getBundleUri(bundle_name)
bundle_uri

# COMMAND ----------

# MAGIC %md ### Read data

# COMMAND ----------

data = spark.read.csv(data_path, header="true", inferSchema="true")
(trainingData, testData) = data.randomSplit([0.7, 0.3], 2019)

# COMMAND ----------

# MAGIC %md ### Read MLeap bundle

# COMMAND ----------

# MAGIC %run ./MLeap_Utils

# COMMAND ----------

model = model = read_model_as_spark_bundle(bundle_uri)

# COMMAND ----------

# MAGIC %md ### Run predictions

# COMMAND ----------

predictions = model.transform(data)

# COMMAND ----------

display(predictions.select(colPrediction, colLabel, colFeatures))