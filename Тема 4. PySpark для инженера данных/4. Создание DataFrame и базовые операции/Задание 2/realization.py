import pyspark
from pyspark.sql import SparkSession



spark = SparkSession.builder \
                    .master("local") \
                    .appName("Learning DataFrames") \
                    .getOrCreate()
path = "/user/master/data/snapshots/channels/actual/*.parquet"
eventsDF = spark.read.load(path)
eventsDF.show(10)

# tSp92pppl6