// Databricks notebook source
// MAGIC %md ## Read MLeap bundle as SparkBundle from filesystem

// COMMAND ----------

// MAGIC %md ### Setup

// COMMAND ----------

// MAGIC %run ../Common

// COMMAND ----------

// MAGIC %run ../SparkBundleUtils

// COMMAND ----------

// MAGIC %md ### Get the MLeap bundle URI

// COMMAND ----------

val bundle = createBundleWidget()
val bundleUri = getBundleUri(bundle)

// COMMAND ----------

// MAGIC %md ### Read data

// COMMAND ----------

val data = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(dataPathDbfs)

// COMMAND ----------

// MAGIC %md ### Read bundle as SparkBundle

// COMMAND ----------

val model = SparkBundleUtils.readModel(bundleUri)

// COMMAND ----------

// MAGIC %md ### Show predictions

// COMMAND ----------

val predictions = model.transform(data)

// COMMAND ----------

println(f"Prediction count: ${predictions.count}")
val sum = predictions.agg(Map("prediction"->"sum")).take(1)(0).getDouble(0)
println(f"Prediction sum:   ${sum}%.3f")

// COMMAND ----------

import org.apache.spark.sql.functions._
display(predictions.groupBy("prediction").count().sort(desc("count")).limit(10))

// COMMAND ----------

display(predictions.select("prediction","quality","features").sort("prediction","quality","features"))