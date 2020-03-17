// Databricks notebook source
// MAGIC %md Utilities for MLeapBundle - No Spark dependencies

// COMMAND ----------

import ml.combust.bundle.BundleFile
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.MleapSupport._
import ml.combust.mleap.runtime.frame.Transformer
import resource._

object MLeapBundleUtils {

  def readModel(bundlePath: String) : Transformer = {
    val bundle = BundleFile(bundlePath)
    try {
      bundle.loadMleapBundle.get.root
    } finally {
      bundle.close()
    }
  }
  
  import ml.combust.mleap.json.JsonSupport._
  import spray.json._
  import scala.io.Source
  
  // Works outside of Databricks, but not in Databricks
  // ERROR: java.lang.NoSuchMethodError: ml.combust.mleap.json.JsonSupport$.MleapStructTypeFormat()Lspray/json/RootJsonFormat;
  /** Read JSON Spark schema and create MLeap schema. */
  def _readSchema(path: String) : StructType = {
    val json = Source.fromFile(path).mkString
    json.parseJson.convertTo[StructType]
  }
  
  // Hack for _readSchema - a TODO. I wish Scala JSON processing was as easy as Python JSON. Sigh. :(
  import org.json4s.jackson.JsonMethods._

  def readSchema(path: String) : StructType = {
    val json = Source.fromFile(path).mkString
    val map = parse(json).values.asInstanceOf[Map[String, Any]]
    val fields = map("fields").asInstanceOf[Seq[Map[String,Any]]]
    val mfields = fields.map(field => {
      val name = field("name").asInstanceOf[String]
      val dtype = field("type").asInstanceOf[String]
      val mtype = dtype match {
        case "integer" => ScalarType.Int
        case "double" => ScalarType.Double
        case "string" => ScalarType.String
        case "_" => throw new Exception("MLeap type '"+dtype+"' not yet supported")
      }
      StructField(name,mtype)
   })
  StructType(mfields).get
  }
}