# MLeap Examples

Basic MLeap examples. 

Demonstrates end-to-end creation of an MLeap bundle and its consumption as a Spark bundle or MLeap bundle.

Synopsis:
* Create an MLeap bundle that can be read by either Spark or MLeap without Spark
* Load and score using SparkBundle
* Load and score using MLeapBundle - no Spark code

Note:
* You can use either the `file:` or `jar:file` scheme for the bundle URI.
  * file:$PWD/../bundles/wine-model 
  * jar:file:$PWD/../bundles/wine-model.zip

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
