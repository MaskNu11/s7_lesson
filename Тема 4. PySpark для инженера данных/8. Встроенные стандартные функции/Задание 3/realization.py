
import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'



import findspark
findspark.init()
findspark.find()

import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F 


spark = SparkSession.builder \
            .master('local') \
            .appName('null_values') \
            .getOrCreate()


path = "/user/master/data/events/date=2022-05-25/"

df = spark.read.json(path)

# df.select('event_type').distinct().show()

df =df.filter(F.col('event_type')=='reaction').groupBy(F.col('event.reaction_from')).count()
# df.orderBy(F.col('count').desc()).limit(1).show()
df.select(F.max('count')).show()

# lXFKRx00xQ