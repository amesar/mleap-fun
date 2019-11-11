package org.andre.mleap.wine

import com.beust.jcommander.{JCommander, Parameter}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc
import org.andre.mleap.MLeapUtils
import org.andre.mleap.wine.Utils._

object SparkMLeapReader {

  def main(args: Array[String]) {
    new JCommander(opts, args.toArray: _*)
    println("Options:")
    println(s"  dataPath: ${opts.dataPath}")
    println(s"  bundlePath: ${opts.bundlePath}")
    
    val spark = SparkSession.builder.appName("Predict").getOrCreate()
    val data = readData(spark,opts.dataPath)

    val model = MLeapUtils.readModelAsSparkBundle(opts.bundlePath)
    val predictions = model.transform(data)
    println("Predictions:")
    predictions.select(colFeatures,colLabel,colPrediction).sort(colFeatures,colLabel,colPrediction).show(10)

    println("Prediction Counts:")
    predictions.groupBy("prediction").count().sort(desc("count")).show

    println("Predictions Schema:")
    predictions.printSchema
  }

  object opts {
    @Parameter(names = Array("--dataPath" ), description = "Data path", required=true)
    var dataPath: String = null

    @Parameter(names = Array("--bundlePath" ), description = "bundlePath", required=true)
    var bundlePath: String = null
  }
}
