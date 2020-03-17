// Databricks notebook source
import org.mlflow.tracking.{MlflowClient,MlflowClientVersion,MlflowHttpException}
import scala.collection.JavaConversions._
println("MLflow version: "+MlflowClientVersion.getClientVersion())

object MlflowUtils {
   
  def getOrCreateExperimentId(client: MlflowClient, experimentName: String) : String = {
    try {
      val experimentId = client.createExperiment(experimentName)
      println(s"Created new experiment: $experimentName")
      experimentId
    } catch { 
      case e: org.mlflow.tracking.MlflowHttpException => { // statusCode 400
        client.getExperimentByName(experimentName).get.getExperimentId
      }
    } 
  } 
  
  def displayRunUri(experimentId: String, runId: String) = {
    val opt = dbutils.notebook.getContext().tags.get("browserHostName")
    val hostName = opt match {
      case Some(h) => h
      case None => "_?_"
    }
    val uri = s"https://$hostName/#mlflow/experiments/$experimentId/runs/$runId"
    displayHTML(s"""Run URI: <a href="$uri">$uri</a>""")
  }
  
  def getLastRunId(client: MlflowClient, experimentName: String) = {
    val experimentId = client.getExperimentByName(experimentName).get.getExperimentId
    val infos = client.listRunInfos(experimentId)
    infos.sortWith(_.getStartTime > _.getStartTime)(0).getRunId()
  }
  
  def _getLastRunId(client: MlflowClient, experimentId: String) = {
    val infos = client.listRunInfos(experimentId)
    val runIds = infos.sortWith(_.getStartTime > _.getStartTime)(0).getRunId()
    runIds(0)
  }
}