// Databricks notebook source
// MAGIC %md ## Write model as MLeap bundle to filesystem

// COMMAND ----------

// MAGIC %md ### Setup

// COMMAND ----------

// MAGIC %run ../Common

// COMMAND ----------

// MAGIC %run ../SparkBundleUtils

// COMMAND ----------

// MAGIC %md ### Read data

// COMMAND ----------

val data = spark.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load(dataPathDbfs)
val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3), 2019)

// COMMAND ----------

// MAGIC %md ### Train Pipeline

// COMMAND ----------

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.DecisionTreeRegressor

// COMMAND ----------

// Setup
val maxDepth = 16
val (colLabel, colPrediction, colFeatures) = ("quality", "prediction", "features")

// Create model
val dt = new DecisionTreeRegressor()
  .setLabelCol(colLabel)
  .setFeaturesCol(colFeatures)
  .setMaxDepth(maxDepth)

// Create pipeline
val columns = data.columns.toList.filter(_ != colLabel)
val assembler = new VectorAssembler()
  .setInputCols(columns.toArray)
  .setOutputCol(colFeatures)
val pipeline = new Pipeline().setStages(Array(assembler,dt))

// Fit model
val model = pipeline.fit(trainingData)

// Transform
val predictions = model.transform(testData)

// COMMAND ----------

// MAGIC %md ### Write model as MLeap bundle

// COMMAND ----------

val bundleUri = "jar:file:" + bundlePathSpark
SparkBundleUtils.writeModel(bundleUri, model, predictions)

// COMMAND ----------

// MAGIC %md ### Write data schema file

// COMMAND ----------

val schemaPath = workDirScala + "/wine-schema.json"
new java.io.PrintWriter(schemaPath) { write(data.schema.json) ; close }

// COMMAND ----------

// MAGIC %md ### Check working files

// COMMAND ----------

// MAGIC %sh ls -l /dbfs/tmp/andre.mesarovic@databricks.com/mleap_demo/scala