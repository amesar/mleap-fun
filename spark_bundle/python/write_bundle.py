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

spark = SparkSession.builder.appName("App").getOrCreate()

def run(data_path, bundle_path):
    # Prepare data
    data = spark.read.csv(data_path, header="true", inferSchema="true")
    (trainingData, testData) = data.randomSplit([0.7, 0.3], 2019)

    # Train pipeline
    dt = DecisionTreeRegressor(labelCol="quality", featuresCol="features", maxDepth=16)
    assembler = VectorAssembler(inputCols=data.columns[:-1], outputCol="features")
    pipeline = Pipeline(stages=[assembler, dt])
    model = pipeline.fit(trainingData)
    predictions = model.transform(testData)
    predictions.show(10,False)

    # Write MLeap bundle
    fs_path = bundle_path.replace("jar:file:","")
    if os.path.exists(fs_path):
        os.remove(fs_path)
    model.serializeToBundle(bundle_path, predictions)

    # Write data schema file
    schema_path = os.path.join(".","wine-schema.json")
    with open(schema_path, 'w') as f:
      f.write(data.schema.json())
    print("schema_path:",schema_path)

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--data_path", dest="data_path", help="data_path", default="../../data/wine-quality-white.csv")
    parser.add_argument("--bundle_path", dest="bundle_path", help="bundle_path", required=True)
    args = parser.parse_args()
    print("Arguments:")
    for arg in vars(args):
        print("  {}: {}".format(arg,getattr(args, arg)))
    run(args.data_path, args.bundle_path)
