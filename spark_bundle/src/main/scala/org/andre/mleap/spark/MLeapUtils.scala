package org.andre.mleap.spark

import ml.combust.bundle.BundleFile
import ml.combust.mleap.spark.SparkSupport._
import ml.combust.mleap.runtime.MleapSupport._
import ml.combust.bundle.dsl.Bundle
import resource.managed
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.DataFrame

object MLeapUtils {
  println("Mleap Bundle version: "+Bundle.version)

  def saveModelAsSparkBundle(bundlePath: String, model: PipelineModel, data: DataFrame) {
    val context = SparkBundleContext().withDataset(data)
    val bundle = BundleFile(bundlePath)
    try {
      model.writeBundle.save(bundle)(context)
    } finally {
      bundle.close()
    }
  }

  def readModelAsSparkBundle(bundlePath: String) = {
    val bundle = BundleFile(bundlePath)
    try {
      bundle.loadSparkBundle().get.root
    } finally {
      bundle.close()
    }
  } 

}
