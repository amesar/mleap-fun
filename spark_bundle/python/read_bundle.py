"""
Read MLeap bundle as SparkBundle
"""

from argparse import ArgumentParser
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from mleap.pyspark.spark_support import SimpleSparkSerializer
from mleap import version

print("MLeap version:",version.__version__)

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--data_path", dest="data_path", help="data_path", default="../../data/wine-quality-white.csv")
    parser.add_argument("--bundle_path", dest="bundle_path", help="bundle_path", required=True)
    args = parser.parse_args()
    print("Arguments:")
    for arg in vars(args):
        print("  {}: {}".format(arg,getattr(args, arg)))

    spark = SparkSession.builder.appName("App").getOrCreate()
    data = spark.read.csv(args.data_path, header="true", inferSchema="true")
    (trainingData, testData) = data.randomSplit([0.7, 0.3], 2019)
    model = PipelineModel.deserializeFromBundle(args.bundle_path)
    predictions = model.transform(data)
    df = predictions.select("prediction", "quality", "features")
    df.show()
