import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.window import Window 
import pyspark.sql.functions as F
    
spark = SparkSession.builder \
                    .master("yarn") \
                    .appName("Learning DataFrames") \
                    .getOrCreate()

events = spark.read.json("/user/master/data/events/date=2022-05-01")


window = Window.partitionBy(['event.message_from']).orderBy('event.datetime')
# window = Window().partitionBy('event. ...').orderBy('...')
# events.select('event.message_to').show(10)
dfWithLag = events.withColumn('lag_7', F.lag('event.message_to', 7).over(window)) \
                .withColumn('date', F.col('event.datetime'))
# dfWithLag = events.withColumn("lag_7",F.lag("event. ... ").over(window))


dfWithLag.show(10, False)
# dfWithLag.select('event.message_from', 'lag_7', 'date').filter('lag_7 is not null AND event.message_from is not null').orderBy(F.col('event.message_from').desc()).show(10, False)

# dfWithLag.select(...) \
# .filter(dfWithLag.lag_7. ...) \
# .orderBy(...) \
# .show(10, False) 
