# Databricks notebook source
# MAGIC %md ## Run All Notebooks

# COMMAND ----------

dbutils.notebook.run("01_Setup.py", 60) 

# COMMAND ----------

# MAGIC %md ### Without MLflow

# COMMAND ----------

dbutils.notebook.run("a_02_Write_MLeap.py", 60) 

# COMMAND ----------

dbutils.notebook.run("a_03_Read_SparkBundle.scala", 60) 

# COMMAND ----------

dbutils.notebook.run("a_04_Read_MLeapBundle.scala", 60) 

# COMMAND ----------

# MAGIC %md ### With MLflow

# COMMAND ----------

dbutils.notebook.run("b_02_Write_MLeap_MLflow", 60) 

# COMMAND ----------

dbutils.notebook.run("b_03_Read_SparkBundle_MLflow.scala", 60) 

# COMMAND ----------

dbutils.notebook.run("b_04_Read_MLeapBundle_MLflow.scala", 60) 