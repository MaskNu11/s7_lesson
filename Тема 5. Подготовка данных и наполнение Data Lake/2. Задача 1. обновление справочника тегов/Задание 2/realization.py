from datetime import datetime, timedelta
from pyspark.sql import SparkSession

# from pyspark.sql.window import Window 
import pyspark.sql.functions as F
# import pandas as pd
import os

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'



# def input_paths(date, date_interval):
#     prod_dir = 'masknu11'
#     dir_list = list()
#     base_input_path = f'/user/{prod_dir}/data/events'
    
#     end_date = datetime.strptime(date, "%Y-%m-%d").date()

#     for interval in range(date_interval):
#         date = end_date - timedelta(days=interval)
#         dir_list.append(f'{base_input_path}/date={date}/event_type=message')   
 
#     return dir_list
# #     messages = spark.read.parquet(*dir_list)
# #     all_tags = messages.where("event.message_channel_to is not null")\
# #         .selectExpr(["event.message_from as user", "explode(event.tags) as tag"])\
# #         .groupBy("tag")\
# #         .agg(F.expr("count(distinct user) as suggested_count"))\
# #         .where("suggested_count >= 100")
    
# #     verified_tags = spark.read.parquet("/user/master/data/snapshots/tags_verified/actual")
# #     candidates = all_tags.join(verified_tags, "tag", "left_anti")
    
# #     candidates.show(10, False)
# def loaded_path():
#     spark = SparkSession.builder \
#                     .master("local") \
#                     .appName("Learning DataFrames") \
#                     .getOrCreate()
    
#     dir_list = input_paths('2022-05-31', 7)
#     print(dir_list)

#     df = spark.read.parquet(*dir_list)
#     df = df.where('event.message_channel_to is not null')\
#         .select(F.explode('event.tags').alias('tag'), F.col('event.message_from').alias('user'))\
#         .groupBy('tag')\
#         .agg(F.countDistinct('user').alias('suggested_count'))\
#         .where(F.col('suggested_count') > 99)
    
#     verified_tags = spark.read.parquet("/user/master/data/snapshots/tags_verified/actual")
#     verified_tags.show(10)

#     candidates = df.join(verified_tags, "tag", "left_anti")

#     candidates.show(10)
    
#     path_save_dir = '/user/masknu11/data/analytics/candidates_d7_pyspark'
    
#     candidates.write\
#         .mode('overwrite')\
#         .parquet(path_save_dir)


# def main():
#     # date, depth = map(str, input().split())

#     loaded_path()


# if __name__ == '__main__':
#     main()
    

def input_paths(date, date_interval):
    prod_dir = 'masknu11'
    dir_list = list()
    base_input_path = f'/user/{prod_dir}/data/events'
    
    end_date = datetime.strptime(date, "%Y-%m-%d").date()

    for interval in range(date_interval):
        date = end_date - timedelta(days=interval)
        dir_list.append(f'{base_input_path}/date={date}/event_type=message')   
    return dir_list
 

spark = SparkSession.builder \
                    .master("local") \
                    .appName("Learning DataFrames") \
                    .getOrCreate()

dir_list = input_paths('2022-05-31', 7)
df = spark.read.parquet(*dir_list)

df = df.where('event.message_channel_to is not null')\
        .select(F.explode('event.tags').alias('tag'), F.col('event.message_from').alias('user'))\
        .groupBy('tag')\
        .agg(F.countDistinct('user').alias('suggested_count'))\
        .where(F.col('suggested_count') > 99)

verified_tags = spark.read.parquet("/user/master/data/snapshots/tags_verified/actual")
candidates = df.join(verified_tags, "tag", "left_anti")
candidates.show(10, False)
path_save_dir = '/user/masknu11/data/analytics/candidates_d7_pyspark'
candidates.write\
        .mode('append')\
        .parquet(path_save_dir)

# input_paths('2022-05-31', 7)
