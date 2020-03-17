# Databricks notebook source
# MAGIC %md ## Write model as MLeap bundle to MLflow
# MAGIC * Log the model as MLeap flavor
# MAGIC * Save the data schema as an artifact

# COMMAND ----------

# MAGIC %md ### Setup

# COMMAND ----------

# MAGIC %run ../Common

# COMMAND ----------

import mlflow
import mlflow.mleap
mlflow.set_experiment(experiment_name_python)
experiment_name_python

# COMMAND ----------

# MAGIC %md ### Read data

# COMMAND ----------

data = spark.read.csv(data_path, header="true", inferSchema="true")
(trainingData, testData) = data.randomSplit([0.7, 0.3], 2019)

# COMMAND ----------

# MAGIC %md ### Train pipeline and log to MLflow

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator

# COMMAND ----------

maxDepth = 16
with mlflow.start_run() as run:
    experiment_id = run.info.experiment_id
    print("Run ID:",run.info.run_id)
    print("Experiment ID:",experiment_id)
    
    # Train our model
    dt = DecisionTreeRegressor(labelCol="quality", featuresCol="features", maxDepth=maxDepth)
    assembler = VectorAssembler(inputCols=data.columns[:-1], outputCol="features")
    pipeline = Pipeline(stages=[assembler, dt])
    model = pipeline.fit(trainingData)
    predictions = model.transform(testData)
    
    # Log standard MLflow params and metrics
    mlflow.log_param("maxDepth",maxDepth)
    evaluator = RegressionEvaluator(labelCol="quality", predictionCol="prediction", metricName="rmse")
    mlflow.log_metric("rmse",evaluator.evaluate(predictions))
    
    # Log the model as MLeap flavor
    mlflow.mleap.log_model(spark_model=model, sample_input=testData, artifact_path="mleap-model")
    
    # Log the schema so we can later use it to create a LeapFrame with MLeapBundle 
    schema_path = "/tmp/schema.json"
    with open(schema_path, 'w') as f:
        f.write(data.schema.json())
    mlflow.log_artifact(schema_path, "mleap-model")

# COMMAND ----------

display(predictions)

# COMMAND ----------

# MAGIC %md ### Show Run UI URI

# COMMAND ----------

display_run_uri(experiment_id, run)