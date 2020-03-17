// Databricks notebook source
// MAGIC %md Utilities MLeapBundle prediction

// COMMAND ----------

import ml.combust.mleap.runtime.frame.{DefaultLeapFrame,Row}
import scala.io.Source

// COMMAND ----------

// MAGIC %run ./MLeapBundleUtils

// COMMAND ----------

def readData(dataPath: String) = {
  import scala.io.Source
  val lines = Source.fromFile(dataPath).getLines.toSeq.drop(1) // skip header
  val data =  lines.map(x => x.split(",").toSeq ).toSeq
  data.map(x => Row(x(0).toDouble,x(1).toDouble,x(2).toDouble, x(3).toDouble, x(4).toDouble, x(5).toDouble, x(6).toDouble, x(7).toDouble, x(8).toDouble, x(9).toDouble, x(10).toDouble,x(11).toInt))
}

// COMMAND ----------

def showSummary(predictions: Seq[Double]) {
  val sum = predictions.sum
  println(f"Prediction count: ${predictions.size}")
  println(f"Prediction sum:   ${sum}%.3f")
}

// COMMAND ----------

def showGroupByCounts(predictions: Seq[Double]) {
  val groups = predictions.groupBy(x => x).mapValues(_.size).toSeq.sortBy(_._2).reverse
  println(s"  prediction    count")
  for (g <- groups.take(10)) { 
    println(f"     ${g._1}%7.3f ${g._2}%8d")
  } 
}

// COMMAND ----------

def showPredictions(predictions: Seq[Double], count: Int=10) {
  println(s"${predictions.size} Predictions:")
  for (p <- predictions.take(count)) { 
      println(f"  $p%7.3f")
  }
}