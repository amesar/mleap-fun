# Databricks notebook source
# MAGIC %md ## README - MLeap Sampler
# MAGIC 
# MAGIC Last updated: 2019-11-20

# COMMAND ----------

# MAGIC %md
# MAGIC #### Overview
# MAGIC * Exercises the many ways to write and read MLeap bundles.
# MAGIC * Write an MLeap bundle (with and without MLflow) and read it in as a SparkBundle or MLeapBundle.
# MAGIC * Uses both Scala and Python except that MLeap does not support Python deserializing.
# MAGIC * When deserializing as MLeapBundle there are *no* Spark dependencies.
# MAGIC * Algorithm is a Spark ML DecisionRegressorTree using the wine quality dataset.
# MAGIC 
# MAGIC #### Details
# MAGIC * Working directory: ``/dbfs/tmp/$USER_NAME/mleap_demo/LANG` where LANG is scala or python.
# MAGIC * For MLeap deserializing, we save the data schema as `mleap-schema.json`. This file is needed to create a LeapFrame from the data.
# MAGIC * For MLflow notebooks
# MAGIC   * MLeap bundle and data schema are stored as artifacts. 
# MAGIC   * The experiments are in your home directory -`MLeap_Sampler_Python` or `MLeap_Sampler_Scala`.
# MAGIC * For non-MLflow notebooks the bundle ans schema are stored on the filesystem
# MAGIC   * Bundle:  `$WORKING_DIR/wine-bundle.zip`.
# MAGIC   * Data schema: `WORKING_DIR/wine-schema.json`
# MAGIC 
# MAGIC #### Notebok Overview
# MAGIC * 01_Write_Bundle - trains a model with SparkML and saves it as an MLeap bundle along with data schema.
# MAGIC * 02_Read_SparkBundle - reads model as a SparkBundle and runs predictions.
# MAGIC * 03_Read_MLeapBundle - reads model as an MLeapBundle and runs predictions using the MLeap runtime.
# MAGIC   * There are *no* Spark dependencies here. 
# MAGIC   * There is no Python MLeap runtime implementation.
# MAGIC 
# MAGIC #### MLflow Scala Notes
# MAGIC * Note that the MLflow Java client (which is used by Scala) does not have an analog of the Python [log_model](https://mlflow.org/docs/latest/python_api/mlflow.spark.html#mlflow.spark.log_model) method.
# MAGIC * So we have to use the lower-level Java [logArtifacts](https://mlflow.org/docs/latest/java_api/org/mlflow/tracking/MlflowClient.html#logArtifact-java.lang.String-java.io.File-) to log models.
# MAGIC   
# MAGIC #### Setup
# MAGIC * [01_Setup]($./01_Setup) - run this before executing any notebook.
# MAGIC * All working files will be created in a directory `/dbfs/$USER_NAME/tmp/mleap_demo`.
# MAGIC   * For example: `/dbfs/tmp/john@doe@acme.com/tmp/mleap_demo`.
# MAGIC * Downloads data to `/dbfs/tmp/john@doe@acme.com/mleap_demo/wine-quality.csv`.

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC #### Notebooks
# MAGIC 
# MAGIC First run [01_Setup]($./01_Setup) after a cluster start. Then run the notebooks in order for any row below.
# MAGIC 
# MAGIC | Language  | MLflow | Write Bundle    | Read as SparkBundle    | Read as MLeapBundle     | 
# MAGIC |:----- |:-----  |:-------------   |:-----               |:----                 |
# MAGIC |Python | no     | [01_Write_Bundle]($./python/no_mlflow/01_Write_Bundle) | [02_Read_SparkBundle]($./python/no_mlflow/02_Read_SparkBundle) | N/A |
# MAGIC |Python | yes    | [01_Write_Bundle]($./python/mlflow/01_Write_Bundle) | [02_Read_SparkBundle]($./python/mlflow/02_Read_SparkBundle) | N/A  |
# MAGIC |Scala  | no     | [01_Write_Bundle]($./scala/no_mlflow/01_Write_Bundle) | [02_Read_SparkBundle]($./scala/no_mlflow/02_Read_SparkBundle) | [03_Read_MLeapBundle]($./scala/no_mlflow/03_Read_MLeapBundle)  |
# MAGIC |Scala  | yes    | [01_Write_Bundle]($./scala/mlflow/01_Write_Bundle) | [02_Read_SparkBundle]($./scala/mlflow/02_Read_SparkBundle) | [03_Read_MLeapBundle]($./scala/mlflow/03_Read_MLeapBundle)  |

# COMMAND ----------

# MAGIC %md
# MAGIC #### Terminology
# MAGIC * MLeap bundle - canonical MLeap bundle format - bits on disk. See [MLeap Bundles](https://mleap-docs.combust.ml/core-concepts/mleap-bundles.html) doc page.
# MAGIC * MLeapBundle - bits read in as an MLeapBundle object for the MLeap runtine - no Spark dependencies.
# MAGIC * SparkBundle - bits read in as a SparkBundle object for standard Spark runtime.
# MAGIC 
# MAGIC #### Extra
# MAGIC * For Scala non-notebook version see - https://github.com/amesar/mleap-fun.