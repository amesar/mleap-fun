# MLeap Examples

Basic MLeap examples. 

Demonstrates end-to-end creation of an MLeap bundle and its consumption as a Spark bundle or MLeap bundle.

#### Synopsis
* Create an MLeap bundle that can be read by either Spark or MLeap without Spark
* Load and score using SparkBundle
* Load and score using MLeapBundle - no Spark code
* DecisionTreeRegressor with wine quality dataset

#### MLeap Bundles
* You can use either the `file:` or `jar:file` scheme for the bundle URI.
  * file:$PWD/../bundles/wine-model 
  * jar:file:$PWD/../bundles/wine-model.zip

#### MLflow MLeap Bundle

Besides the bundle generated by [SparkMLeapWriter.scala](spark_bundle/src/main/scala/org/andre/mleap/wine/SparkMLeapWriter.scala),
you can point to a bundle generated by MLflow.

[SparkMLeapReader.scala](spark_bundle/src/main/scala/org/andre/mleap/wine/SparkMLeapReader.scala) and [MLeapReader.scala](mleap_bundle/src/main/scala/org/andre/mleap/wine/MLeapReader.scala) can read the following identical model from MLflow.
  * https://github.com/amesar/mlflow-examples/tree/master/scala_spark
  * Sample bundle path: file:$HOME/mlflow/server/mlruns/2/853cd54adb3f4eabb94807b6aa9f8a0f/artifacts/mleap-model/mleap/model

#### Issues

The model loaded by SparkMLeapReader.scala does not score correctly as all predictions are 0.
If the jar is loaded into a Databricks ML 6.1 the program does score correctly.

## Setup

* Install Spark 2.3.0.
* Install Scala 2.11.8.

## SparkBundle
**Build jar**
```
cd spark_bundle
mvn clean package
```

**Create bundle**

SparkMLeapWriter will create a schema file for the data in a Spark JSON schema format. 
This file will be used by the MLeap bundle reader.
See [samples/wine_schema.json](samples/wine_schema.json).

```
spark-submit \
  --class org.andre.mleap.wine.SparkMLeapWriter \
  --master local[2] \
  target/mleap-spark-examples-1.0-SNAPSHOT.jar \
  --bundlePath file:$PWD/../bundles/wine-model \
  --dataPath ../data/wine-quality-white.csv \
  --schemaPath ../wine_schema.json
```

**Read and score Spark bundle**
```
spark-submit \
  --class org.andre.mleap.wine.SparkMLeapReader \
  --master local[2] \
  target/mleap-spark-examples-1.0-SNAPSHOT.jar \
  --bundlePath file:$PWD/../bundles/wine-model \
  --dataPath ../data/wine-quality-white.csv
```

## MleapBundle

This module does not contain any Spark code.
MLeapReader uses the schema file created by SparkMLeapWriter.

**Build jar**
```
cd mleap_bundle
mvn clean package
```

**Read and score MLeap bundle**
```
scala \
  -cp target/mleap-examples-1.0-SNAPSHOT.jar \
  org.andre.mleap.wine.MLeapReader \
  --dataPath ../data/wine-quality-white.csv \
  --bundlePath file:$PWD/../bundles/wine-model \
  --schemaPath ../wine_schema.json
```
