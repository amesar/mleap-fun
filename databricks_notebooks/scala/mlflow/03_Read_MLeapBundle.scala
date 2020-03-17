// Databricks notebook source
// MAGIC %md ## Read MLeap bundle as MLeapBundle from MLflow 
// MAGIC * No Spark dependencies.
// MAGIC * Widget `Bundle` - which language was used to generate bundle.

// COMMAND ----------

// MAGIC %md ### Setup

// COMMAND ----------

// MAGIC %run ../Common

// COMMAND ----------

// MAGIC %run ../MLeapPredictUtils

// COMMAND ----------

// MAGIC %run ./MLflowUtils

// COMMAND ----------

import org.mlflow.tracking.{MlflowClient,MlflowClientVersion}
val client = new MlflowClient()
println("MLflow version: "+MlflowClientVersion.getClientVersion())

// COMMAND ----------

// MAGIC %md ### Get the last MLflow run ID

// COMMAND ----------

val bundle = createBundleWidget()
val experimentName = getExperimentNameForBundle(bundle) 
val runId = MlflowUtils.getLastRunId(client, experimentName)

// COMMAND ----------

// MAGIC %md ### Read Schema
// MAGIC 
// MAGIC Read MLeap schema from the run's artifact.

// COMMAND ----------

val schemaPath = client.downloadArtifacts(runId,"mleap-model/schema.json").getAbsolutePath

// COMMAND ----------

val schema = MLeapBundleUtils.readSchema(schemaPath)

// COMMAND ----------

// MAGIC %md ### Read Data

// COMMAND ----------

val records = readData(dataPath)
val data = DefaultLeapFrame(schema, records)

// COMMAND ----------

// MAGIC %md ### Read bundle as MLeapBundle

// COMMAND ----------

val modelPath = client.downloadArtifacts(runId,"mleap-model/mleap/model").getAbsolutePath
val bundleUri = s"file:${modelPath}"

// COMMAND ----------

// MAGIC %md ### Run predictions

// COMMAND ----------

val model = MLeapBundleUtils.readModel(bundleUri)

// COMMAND ----------

val transformed = model.transform(data).get

// COMMAND ----------

val predictions = transformed.select("prediction").get.dataset.map(p => p.getDouble(0))

// COMMAND ----------

showSummary(predictions)

// COMMAND ----------

showGroupByCounts(predictions)

// COMMAND ----------

showPredictions(predictions)