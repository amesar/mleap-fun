"""
Write SparkML model as MLeap bundle.
"""
import os
from argparse import ArgumentParser
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.feature import VectorAssembler
from mleap.pyspark.spark_support import SimpleSparkSerializer

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--data_path", dest="data_path", help="data_path", default="../../data/wine-quality-white.csv")
    parser.add_argument("--bundle_path", dest="bundle_path", help="bundle_path", required=True)
    args = parser.parse_args()
    print("Arguments:")
    for arg in vars(args):
        print("  {}: {}".format(arg,getattr(args, arg)))
    
    # Prepare data
    spark = SparkSession.builder.appName("App").getOrCreate()
    data = spark.read.csv(args.data_path, header="true", inferSchema="true")
    (trainingData, testData) = data.randomSplit([0.7, 0.3], 2019)

    # Train pipeline
    dt = DecisionTreeRegressor(labelCol="quality", featuresCol="features", maxDepth=16)
    assembler = VectorAssembler(inputCols=data.columns[:-1], outputCol="features")
    pipeline = Pipeline(stages=[assembler, dt])
    model = pipeline.fit(trainingData)
    predictions = model.transform(testData)
    predictions.show(10,False)

    # Write MLeap bundle
    fs_path = args.bundle_path.replace("jar:file:","")
    if os.path.exists(fs_path):
        os.remove(fs_path)
    model.serializeToBundle(args.bundle_path, predictions)
    #model.serializeToBundle(args.bundle_path, data) # NOTE: fails with error. In Scala this works.

    # Write data schema file
    schema_path = "wine-schema.json"
    with open(schema_path, 'w') as f:
      f.write(data.schema.json())
    print("schema_path:",schema_path)