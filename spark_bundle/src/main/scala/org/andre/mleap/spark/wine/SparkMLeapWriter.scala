package org.andre.mleap.spark.wine

import java.io.{File,PrintWriter}
import org.apache.spark.sql.{SparkSession,DataFrame}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.ml.evaluation.RegressionEvaluator
import com.beust.jcommander.{JCommander, Parameter}
import ml.combust.mleap.core.types._
import org.andre.mleap.spark.{MLeapUtils,CommonUtils}
import org.andre.mleap.spark.wine.Utils._

object SparkMLeapWriter {
  val spark = SparkSession.builder.appName("DecisionTreeRegressionExample").getOrCreate()
  println("Spark version: "+spark.version)

  def main(args: Array[String]) {
    new JCommander(opts, args: _*)
    println("Options:")
    println(s"  dataPath: ${opts.dataPath}")
    println(s"  bundlePath: ${opts.bundlePath}")
    println(s"  schemaPath: ${opts.schemaPath}")
    println(s"  maxDepth: ${opts.maxDepth}")
    println(s"  maxBins: ${opts.maxBins}")
    val dataHolder = prepareData(spark, opts.dataPath, opts.schemaPath)
    train(opts.bundlePath, dataHolder, opts.maxDepth, opts.maxBins)
  }

  def train(bundlePath: String, dataHolder: DataHolder, maxDepth: Int, maxBins: Int) {
   val dt = new DecisionTreeRegressor()
      .setLabelCol(colLabel)
      .setFeaturesCol(colFeatures)
      .setMaxDepth(maxDepth)
      .setMaxBins(maxBins)

    val pipeline = new Pipeline().setStages(Array(dataHolder.assembler,dt))
    val model = pipeline.fit(dataHolder.trainingData)

    val predictions = model.transform(dataHolder.testData)
    println("Predictions Schema:")
    predictions.printSchema()

    println("Metrics:")
    val metrics = Seq("rmse","r2", "mae")
    for (metric <- metrics) { 
      val evaluator = new RegressionEvaluator()
        .setLabelCol(colLabel)
        .setPredictionCol(colPrediction)
        .setMetricName(metric)
      val v = evaluator.evaluate(predictions)
      println(f"  $metric%-4s: $v%.3f - isLargerBetter: ${evaluator.isLargerBetter}")
    } 

    CommonUtils.createOutputDir(bundlePath)
    MLeapUtils.saveModelAsSparkBundle(bundlePath, model, predictions)
  }

  object opts {
    @Parameter(names = Array("--dataPath" ), description = "Data path", required=true)
    var dataPath: String = null

    @Parameter(names = Array("--bundlePath" ), description = "Data path", required=true)
    var bundlePath: String = null

    @Parameter(names = Array("--schemaPath" ), description = "schemaPath", required=true)
    var schemaPath: String = null

    @Parameter(names = Array("--maxDepth" ), description = "maxDepth param", required=false)
    var maxDepth: Int = 5 // per doc

    @Parameter(names = Array("--maxBins" ), description = "maxBins param", required=false)
    var maxBins: Int = 32 // per doc
  }
}
