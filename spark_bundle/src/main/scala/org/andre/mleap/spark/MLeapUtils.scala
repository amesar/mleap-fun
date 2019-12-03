package org.andre.mleap.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.{PipelineModel,Transformer}
import org.apache.spark.ml.bundle.SparkBundleContext
import ml.combust.bundle.BundleFile
import ml.combust.bundle.dsl.Bundle
import ml.combust.mleap.spark.SparkSupport._

object MLeapUtils {
  println("Mleap Bundle version: "+Bundle.version)

  def writeModel(bundlePath: String, model: PipelineModel, data: DataFrame) {
    deleteBundle(bundlePath)
    val context = SparkBundleContext().withDataset(data)
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

  def deleteBundle(bundlePath: String) {
    if (bundlePath.startsWith("jar:file:")) {
      val path = new java.io.File(bundlePath.replace("jar:file:",""))
      path.delete
    }
  }
}
