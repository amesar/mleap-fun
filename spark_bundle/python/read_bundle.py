"""
Read MLeap bundle as SparkBundle
"""
import os
from argparse import ArgumentParser
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from mleap.pyspark.spark_support import SimpleSparkSerializer

spark = SparkSession.builder.appName("App").getOrCreate()

def run(data_path, bundle_path):
    # Prepare data
    data = spark.read.csv(data_path, header="true", inferSchema="true")
    (trainingData, testData) = data.randomSplit([0.7, 0.3], 2019)
    model = PipelineModel.deserializeFromBundle(bundle_path)
    predictions = model.transform(data)
    df = predictions.select("prediction", "quality", "features")
    df.show()

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--data_path", dest="data_path", help="data_path", default="../../data/wine-quality-white.csv")
    parser.add_argument("--bundle_path", dest="bundle_path", help="bundle_path", required=True)
    args = parser.parse_args()
    print("Arguments:")
    for arg in vars(args):
        print("  {}: {}".format(arg,getattr(args, arg)))
    run(args.data_path, args.bundle_path)
