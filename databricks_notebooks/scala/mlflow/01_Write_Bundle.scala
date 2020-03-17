// Databricks notebook source
// MAGIC %md ## Write model as MLeap bundle to MLflow

// COMMAND ----------

// MAGIC %md ### Setup

// COMMAND ----------

// MAGIC %run ../Common

// COMMAND ----------

// MAGIC %run ../SparkBundleUtils

// COMMAND ----------

// MAGIC %run ./MLflowUtils

// COMMAND ----------

// MAGIC %md ### Read data

// COMMAND ----------

val data = spark.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load(dataPathDbfs)
val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3), 2019)

// COMMAND ----------

// MAGIC %md ### Train Pipeline

// COMMAND ----------

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.DecisionTreeRegressor

// COMMAND ----------

// Setup
val maxDepth = 16
val (colLabel, colPrediction, colFeatures) = ("quality", "prediction", "features")

// Create model
val dt = new DecisionTreeRegressor()
  .setLabelCol(colLabel)
  .setFeaturesCol(colFeatures)
  .setMaxDepth(maxDepth)

// Create pipeline
val columns = data.columns.toList.filter(_ != colLabel)
val assembler = new VectorAssembler()
  .setInputCols(columns.toArray)
  .setOutputCol(colFeatures)
val pipeline = new Pipeline().setStages(Array(assembler,dt))

// Fit model
val model = pipeline.fit(trainingData)

// Transform
val predictions = model.transform(testData)

// COMMAND ----------

// MAGIC %md ### Log run to MLflow

// COMMAND ----------

// MAGIC %md #### <a>MLflow Setup</a>

// COMMAND ----------

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.mlflow.api.proto.Service.RunStatus
import org.apache.commons.io.FileUtils

// COMMAND ----------

import org.mlflow.tracking.{MlflowClient,MlflowClientVersion}
val client = new MlflowClient()
MlflowClientVersion.getClientVersion()

// COMMAND ----------

val experimentId = MlflowUtils.getOrCreateExperimentId(client, experimentNameScala)
experimentNameScala

// COMMAND ----------

// MAGIC %md #### <a>Create MLflow Run</a>

// COMMAND ----------

val runInfo = client.createRun(experimentId)
val runId = runInfo.getRunId()
println(s"Run ID: $runId")

// COMMAND ----------

// MAGIC %md #### <a>Log params, metrics and tags</a>

// COMMAND ----------

// Log MLflow tags
client.setTag(runId, "mlflowVersion", MlflowClientVersion.getClientVersion())

// Log MLflow parameters
client.logParam(runId, "maxDepth",""+maxDepth)

// Log MLflow metrics
val predictions = model.transform(testData)
for (metric <- Seq("rmse","r2", "mae")) { 
  val evaluator = new RegressionEvaluator()
    .setLabelCol(colLabel)
    .setPredictionCol(colPrediction)
    .setMetricName(metric)
  client.logMetric(runId, metric, evaluator.evaluate(predictions))
} 

// COMMAND ----------

// MAGIC %md #### <a>Write MLflow model as MLeap artifact</a>

// COMMAND ----------

val modelDir = new java.io.File(s"$workDirScala/tmp/spark-model")
FileUtils.deleteDirectory(modelDir)
SparkBundleUtils.writeModel("file:"+modelDir.getAbsolutePath, model, predictions)
client.logArtifacts(runId, modelDir, "mleap-model/mleap/model") // NOTE: Make compatible with MLflow Python mlflow.mleap.log_model  

// COMMAND ----------

// MAGIC %md #### <a>Log data schema for MLeapBundle consumption</a>
// MAGIC * Log the schema so we can later use it to create a LeapFrame with MLeapBundle 

// COMMAND ----------

val schemaPath = new java.io.File(s"$workDirScala/tmp/schema.json")
new java.io.PrintWriter(schemaPath) { write(data.schema.json) ; close }
client.logArtifact(runId, schemaPath, "mleap-model") 

// COMMAND ----------

// MAGIC %md #### <a>Close Run</a>

// COMMAND ----------

client.setTerminated(runId, RunStatus.FINISHED, System.currentTimeMillis())

// COMMAND ----------

// MAGIC %md ### Display MLflow Run UI URI

// COMMAND ----------

MlflowUtils.displayRunUri(experimentId,runId)

// COMMAND ----------

// MAGIC %md ### Check files

// COMMAND ----------

// MAGIC %sh ls -l /dbfs/tmp/andre.mesarovic@databricks.com/mleap_demo/scala/tmp