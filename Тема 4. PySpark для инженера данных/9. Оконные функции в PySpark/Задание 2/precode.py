import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
                    .master("local") \
                    .appName("Learning DataFrames") \
                    .getOrCreate()

path = "/user/master/data/events/date=2022-05-01/"
events = spark.read.json(path)
# events.show(10, False)
events.printSchema()