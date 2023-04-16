# прекод для выполнения задания
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F 
# from pyspark import SparkContext,SparkConf


spark = SparkSession.builder \
                    .master("local") \
                    .appName("Learning DataFrames") \
                    .getOrCreate()

path = "/user/master/data/events/date=2022-05-31/"

events = spark.read.json(path)
# events.show(10)

events_curr_day = events.withColumn('hours', F.hour(F.col('event.datetime'))) \
                        .withColumn('minute', F.minute(F.col('event.datetime'))) \
                        .withColumn('second', F.second(F.col('event.datetime'))) \
                        .orderBy(F.col('event.datetime').desc())


events_curr_day.show(10)

# 5T3T8hAzm0