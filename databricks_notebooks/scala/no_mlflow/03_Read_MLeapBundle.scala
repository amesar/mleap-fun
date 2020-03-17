// Databricks notebook source
// MAGIC %md ## Read MLeap bundle as SparkBundle from filesystem
// MAGIC * No Spark dependencies.
// MAGIC * Widget `Bundle` - which language was used to generate bundle.

// COMMAND ----------

// MAGIC %md ### Setup

// COMMAND ----------

// MAGIC %run ../Common

// COMMAND ----------

// MAGIC %run ../MLeapPredictUtils

// COMMAND ----------

val bundle = createBundleWidget()
val bundleUri = getBundleUri(bundle)

// COMMAND ----------

// MAGIC %md ### Read Schema

// COMMAND ----------

val schemaPath = getSchemaPath(bundle)
val schema = MLeapBundleUtils.readSchema(schemaPath)

// COMMAND ----------

// MAGIC %md ### Read Data

// COMMAND ----------

val records = readData(dataPath)
val data = DefaultLeapFrame(schema, records)

// COMMAND ----------

// MAGIC %md ### Run predictions

// COMMAND ----------

val model = MLeapBundleUtils.readModel(bundleUri)

// COMMAND ----------

val transformed = model.transform(data).get
val predictions = transformed.select("prediction").get.dataset.map(p => p.getDouble(0))

// COMMAND ----------

// MAGIC %md ### Show predictions

// COMMAND ----------

showSummary(predictions)

// COMMAND ----------

showGroupByCounts(predictions)

// COMMAND ----------

showPredictions(predictions)