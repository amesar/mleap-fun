// Databricks notebook source
// MAGIC %md Common Scala utilities

// COMMAND ----------

val user = dbutils.notebook.getContext().tags("user")

val workDir = s"/dbfs/tmp/${user}/mleap_demo"
val workDirScala = workDir + "/scala"
val workDirPython = workDir + "/python"

val dataPath  = workDir + "/wine-quality.csv"
val dataPathDbfs  = workDir.replace("/dbfs","dbfs:") + "/wine-quality.csv"

val bundlePathPython = workDirPython + "/wine-bundle.zip"
val bundlePathSpark = workDirScala + "/wine-bundle.zip"

// COMMAND ----------

def getSchemaPath(bundleName: String) = {
  val dir = if (bundleName=="Scala") workDirScala else workDirPython
  dir + "/wine-schema.json"
}

// COMMAND ----------

def createBundleWidget(name: String = "Bundle") = {
  dbutils.widgets.dropdown(name,"Scala",Seq("Scala","Python"))
  dbutils.widgets.get(name)
}

// COMMAND ----------

def getBundlePath(bundleName: String) = if (bundleName=="Scala") bundlePathSpark else bundlePathPython
def getBundleUri(bundleName: String) = "jar:file:" + getBundlePath(bundleName)

// COMMAND ----------

val experimentNameBase = s"/Users/${user}/MLeap_Sampler"
val experimentNameScala = s"${experimentNameBase}_Scala"

def getExperimentNameForBundle(bundle: String) = s"${experimentNameBase}_${bundle}"