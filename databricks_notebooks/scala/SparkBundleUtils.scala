// Databricks notebook source
// MAGIC %md Utilities for SparkBundle

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.{PipelineModel,Transformer}
import org.apache.spark.ml.bundle.SparkBundleContext
import ml.combust.bundle.BundleFile
import ml.combust.mleap.spark.SparkSupport._

object SparkBundleUtils {
  
  def writeModel(bundlePath: String, model: PipelineModel, df: DataFrame) {
    val context = SparkBundleContext().withDataset(df)
    val bundle = BundleFile(bundlePath)
    try {
      model.writeBundle.save(bundle)(context)
    } finally {
      bundle.close()
    }
  }

  def readModel(bundlePath: String) : Transformer = {
    val bundle = BundleFile(bundlePath)
    try {
      bundle.loadSparkBundle().get.root
    } finally {
      bundle.close()
    }
  }

}

// COMMAND ----------

