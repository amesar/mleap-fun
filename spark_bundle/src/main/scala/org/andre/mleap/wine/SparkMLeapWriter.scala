package org.andre.mleap.wine

import java.io.{File,PrintWriter}
import org.apache.spark.sql.{SparkSession,DataFrame}
import org.apache.spark.ml.feature.VectorAssembler
//import org.apache.spark.ml.feature.{VectorIndexer,VectorIndexerModel}
import org.apache.spark.ml.Pipeline
//import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.DecisionTreeRegressor
import com.beust.jcommander.{JCommander, Parameter}
import ml.combust.mleap.core.types._
import org.andre.mleap.MLeapUtils
import org.andre.mleap.wine.Utils._

case class DataHolder(trainingData: DataFrame, testData: DataFrame, assembler: VectorAssembler)

object SparkMLeapWriter {
  val spark = SparkSession.builder.appName("DecisionTreeRegressionExample").getOrCreate()

  def main(args: Array[String]) {
    new JCommander(opts, args: _*)
    println("Options:")
    println(s"  dataPath: ${opts.dataPath}")
    println(s"  bundlePath: ${opts.bundlePath}")
    println(s"  schemaPath: ${opts.schemaPath}")
    val dataHolder = prepareData(opts.dataPath, opts.schemaPath)
    train(opts.bundlePath, dataHolder)
  }

  def prepareData(dataPath: String, schemaPath: String) : DataHolder = {
    val data = readData(spark, dataPath)

    data.printSchema
    scala.tools.nsc.io.File(schemaPath).writeAll(data.schema.json)

    val columns = data.columns.toList.filter(_ != colLabel)
    val assembler = new VectorAssembler()
      .setInputCols(columns.toArray)
      .setOutputCol("features")
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3), 2019)
    DataHolder(trainingData, testData, assembler)
  }

  def train(bundlePath: String, dataHolder: DataHolder) {
   val dt = new DecisionTreeRegressor()
      .setLabelCol(colLabel)
      .setFeaturesCol(colFeatures)

    val pipeline = new Pipeline().setStages(Array(dataHolder.assembler,dt))
    val model = pipeline.fit(dataHolder.trainingData)

    val predictions = model.transform(dataHolder.testData)
    println("Predictions Schema:")
    predictions.printSchema()

    createOutputDir(bundlePath)
    MLeapUtils.saveModelAsSparkBundle(bundlePath, model, predictions)
  }

  def createOutputDir(bundlePath: String) {
    if (bundlePath.startsWith("file:")) {
      val path = bundlePath.replace("file:","")
      (new File(path)).mkdirs
    } else if (bundlePath.startsWith("jar:")) {
      val path = bundlePath.replace("jar:file:","")
      (new File(path)).getParentFile.mkdirs
    } else {
      throw new Exception(s"Bad bundle URI: $bundlePath")
    }
  }

  object opts {
    @Parameter(names = Array("--dataPath" ), description = "Data path", required=true)
    var dataPath: String = null

    @Parameter(names = Array("--bundlePath" ), description = "Data path", required=true)
    var bundlePath: String = null

    @Parameter(names = Array("--schemaPath" ), description = "schemaPath", required=true)
    var schemaPath: String = null
  }
}
