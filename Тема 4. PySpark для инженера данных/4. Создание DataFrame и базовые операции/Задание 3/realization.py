import pyspark
from pyspark.sql import SparkSession



spark = SparkSession.builder \
                    .master("local") \
                    .appName("Learning DataFrames") \
                    .getOrCreate()
path = "/user/master/data/snapshots/channels/actual/*.parquet"

# eventsDF = spark.read.load(path)
# eventsDF.show(10)

# eventsDF.write.format('parquet') \
#         .partitionBy('channel_type') \
#         .mode('append') \
#         .parquet('/user/masknu11/analytics/test')
# #         .save('/user/masknu11/analytics/test')

path = '/user/masknu11/analytics/test'
df = spark.read.load(path)
df.select('channel_type').orderBy('channel_type').distinct().show()

# 3TkchiZud0