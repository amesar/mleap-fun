# Databricks notebook source
# MAGIC %md ## Write model as MLeap bundle to filesystem

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

# MAGIC %md ### Train pipeline

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.feature import VectorAssembler

maxDepth = 16
dt = DecisionTreeRegressor(labelCol="quality", featuresCol="features", maxDepth=maxDepth)
assembler = VectorAssembler(inputCols=data.columns[:-1], outputCol="features")
pipeline = Pipeline(stages=[assembler, dt])

model = pipeline.fit(trainingData)
predictions = model.transform(testData)

# COMMAND ----------

display(predictions)

# COMMAND ----------

# MAGIC %md ### Write MLeap bundle

# COMMAND ----------

if os.path.exists(bundle_path_python):
    os.remove(bundle_path_python)
bundle_path_python

# COMMAND ----------

from mleap.pyspark.spark_support import SimpleSparkSerializer
model.serializeToBundle("jar:file:"+bundle_path_python, predictions)

# COMMAND ----------

# MAGIC %md ### Write data schema file

# COMMAND ----------

schema_path = os.path.join(work_dir_python,"wine-schema.json")
with open(schema_path, "w") as f:
    f.write(data.schema.json())
schema_path

# COMMAND ----------

# MAGIC %md ### Check working files

# COMMAND ----------

# MAGIC %sh echo $WORK_DIR

# COMMAND ----------

# MAGIC %sh ls -l $WORK_DIR/python