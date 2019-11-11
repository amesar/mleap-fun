package org.andre.mleap.wine

import com.beust.jcommander.{JCommander, Parameter}
import ml.combust.mleap.runtime.frame.Row
import ml.combust.mleap.runtime.frame.DefaultLeapFrame
import org.andre.mleap.MLeapUtils

object MLeapReader {

  def main(args: Array[String]) {
    new JCommander(opts, args.toArray: _*)
    println("Options:")
    println(s"  dataPath: ${opts.dataPath}")
    println(s"  bundlePath: ${opts.bundlePath}")
    println(s"  schemaPath: ${opts.schemaPath}")

    val schema = MLeapUtils.readSchema(opts.schemaPath)
    val lst = readData(opts.dataPath)
    val data = DefaultLeapFrame(schema, lst)

    val model = MLeapUtils.readModelAsMLeapBundle(opts.bundlePath)
    println(s"Model class: ${model.getClass.getName}")

    val transformed = model.transform(data).get
    val predictions = transformed.select("prediction").get.dataset.map(p => p.getDouble(0))

    println(s"${predictions.size} Predictions:")
    for (p <- predictions.take(10)) {
      println(f"  $p%5.3f")
    }
    val sum = predictions.sum
    println(f"Prediction sum: ${sum}%.3f")

    val groups = predictions.groupBy(x => x).mapValues(_.size).toSeq
    println(s"\nprediction count")
    for (g <- groups) {
      println(f"     ${g._1}%5.3f ${g._2}%5d")
    }
  }

  def readData(dataPath: String) = {
    import scala.io.Source
    val lines = Source.fromFile(dataPath).getLines.toSeq.drop(1)
    val lst =  lines.map(x => x.split(",").toSeq ).toSeq
    lst.map(x => Row(x(0).toDouble,x(1).toDouble,x(2).toDouble, x(3).toDouble, x(4).toDouble, x(5).toDouble, x(6).toDouble, x(7).toDouble, x(8).toDouble, x(9).toDouble, x(10).toDouble,x(11).toInt))
  } 

  object opts {
    @Parameter(names = Array("--dataPath" ), description = "Data path", required=true)
    var dataPath: String = null

    @Parameter(names = Array("--bundlePath" ), description = "bundlePath", required=true)
    var bundlePath: String = null

    @Parameter(names = Array("--schemaPath" ), description = "schemaPath", required=true)
    var schemaPath: String = null
  }
}
