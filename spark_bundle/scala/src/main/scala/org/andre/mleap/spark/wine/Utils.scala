package org.andre.mleap.spark.wine

import org.apache.spark.sql.{SparkSession,DataFrame}
import org.apache.spark.ml.feature.VectorAssembler

case class DataHolder(trainingData: DataFrame, testData: DataFrame, assembler: VectorAssembler)

object Utils {
  val colLabel = "quality"
  val colPrediction = "prediction"
  val colFeatures = "features"

  def readData(spark: SparkSession, dataPath: String) = {
    spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(dataPath)
  }

  def prepareData(spark: SparkSession, dataPath: String, schemaPath: String) : DataHolder = {
    val data = readData(spark, dataPath)
    println("Data Schema:")
    data.printSchema
    scala.tools.nsc.io.File(schemaPath).writeAll(data.schema.json)
    val columns = data.columns.toList.filter(_ != colLabel)
    val assembler = new VectorAssembler()
      .setInputCols(columns.toArray)
      .setOutputCol("features")
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3), 2019)
    DataHolder(trainingData, testData, assembler)
  }
}
