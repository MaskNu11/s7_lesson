import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F 


spark = SparkSession.builder \
            .master('local') \
            .appName('null_values') \
            .getOrCreate()


path = "/user/master/data/events/date=2022-05-31/"

df = spark.read.json(path)

df.filter(F.col('event.message_to').isNotNull()).count()

# XUdp1wiOxP