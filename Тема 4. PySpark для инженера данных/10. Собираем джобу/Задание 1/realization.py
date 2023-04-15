from pyspark.sql import SparkSession
from pyspark.sql.window import Window

import pyspark
import pyspark.sql.functions as F


spark = SparkSession.builder \
            .master('local') \
            .appName('load_data_layer_ods') \
            .getOrCreate()

path = '/user/master/data/events/'
df = spark.read.json(path)

path_write = '/user/masknu11/data/events'
df.write.format('parquet').mode('append').save(path_write)

df_ods = spark.read.load(path_write)


df_ods.select('event', 'event.datetime', 'event_type').withColumnRenamed('datetime', 'date').filter('event.datetime is not null').orderBy(F.col('event.datetime').desc()).show(10)