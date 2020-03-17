// Databricks notebook source
// MAGIC %md ## Read MLeap bundle as a SparkBundle from MLflow
// MAGIC * Widget `bundle` - which language was used to generate bundle.

// COMMAND ----------

// MAGIC %md ### Setup

// COMMAND ----------

// MAGIC %run ../Common

// COMMAND ----------

// MAGIC %run ../SparkBundleUtils

// COMMAND ----------

// MAGIC %run ./MLflowUtils

// COMMAND ----------

// MAGIC %md ### Get the last MLflow run ID

// COMMAND ----------

import org.mlflow.tracking.{MlflowClient,MlflowContext,MlflowClientVersion}
val client = new MlflowClient()
println("MLflow version: "+MlflowClientVersion.getClientVersion())

// COMMAND ----------

val bundle = createBundleWidget()
val experimentName = getExperimentNameForBundle(bundle) 
val runId = MlflowUtils.getLastRunId(client, experimentName)

// COMMAND ----------

// MAGIC %md ### Read data

// COMMAND ----------

val data = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(dataPathDbfs)

// COMMAND ----------

// MAGIC %md ### Read bundle as SparkBundle

// COMMAND ----------

val modelPath = client.downloadArtifacts(runId,"mleap-model/mleap/model").getAbsolutePath

// COMMAND ----------

val bundlePath = s"file:${modelPath}"
val model = SparkBundleUtils.readModel(bundlePath)

// COMMAND ----------

// MAGIC %md ### Run predictions

// COMMAND ----------

val predictions = model.transform(data)

// COMMAND ----------

// MAGIC %md ### Show predictions

// COMMAND ----------

println(f"Prediction count: ${predictions.count}")
val sum = predictions.agg(Map("prediction"->"sum")).take(1)(0).getDouble(0)
println(f"Prediction sum:   ${sum}%.3f")

// COMMAND ----------

import org.apache.spark.sql.functions._
predictions.groupBy("prediction").count().sort(desc("count")).limit(10).show

// COMMAND ----------

display(predictions.select("prediction","quality","features").sort("prediction","quality","features"))