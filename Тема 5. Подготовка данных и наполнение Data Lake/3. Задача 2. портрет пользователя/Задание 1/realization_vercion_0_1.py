
from datetime import datetime, timedelta
from pyspark.sql.window import Window 
from pyspark.sql import SparkSession

import pyspark.sql.functions as F


def input_paths(date, day_interval, base_input_path):
    dir_list = list()
    end_date = datetime.strptime(date, '%Y-%m-%d').date()

    for interval in range(day_interval):
        date = end_date - timedelta(days=interval)
        dir_list.append(f'{base_input_path}/date={date}/event_type=message')
    
    return dir_list 

def tag_tops(date, day_interval, spark):
    base_input_path = '/user/masknu11/data/events'
    
    dir_list = input_paths(date, day_interval, base_input_path)
    print(dir_list)
    df = spark.read.parquet(*dir_list)
    
    df.printSchema()
    
    window = Window.partitionBy('user_id').orderBy(F.col('tag').desc(), F.col('count').desc())
    
#     df1 = df.select('event.message_id', F.col('event.message_from').alias('user_id'), F.explode('event.tags').alias('tag'))\
#         .groupBy('user_id', 'tag')\
#         .agg(F.count('*').alias('count'))\
#         .orderBy(F.col('count').desc())\
#         .withColumn('rating', F.row_number().over(window))\
#         .where(F.col('rating') <= 3)

#     df_top_tags = df1.groupBy('user_id')\
#                     .pivot('rating') \
#                     .agg(F.first('tag'))\
#                     .withColumnRenamed('1', 'tag_top_1')\
#                     .withColumnRenamed('2', 'tag_top_2')\
#                     .withColumnRenamed('3', 'tag_top_3')\
# #                     .show(10)
    
    window = Window.partitionBy('user_id').orderBy(F.desc('tag'), F.desc('count'))
    
    df_top_tags = (df
            .select(F.col('event.message_from').alias('user_id'), F.explode('event.tags').alias('tag'))
            .groupBy('user_id', 'tag')
            .agg(F.count('tag').alias('count'))
            .orderBy(F.col("count").desc())
            .withColumn('rating', F.row_number().over(window))
            .where(F.col('rating') <= 3)
            .groupBy('user_id')
            .pivot('rating')
            .agg(F.collect_set('tag')[0])
            .withColumnRenamed('1', 'df_tag_top_1')
            .withColumnRenamed('2', 'df_tag_top_2')
            .withColumnRenamed('3', 'df_tag_top_3')
                  )

    window = Window().partitionBy('user_id').orderBy(F.col('count(tag)').desc(), F.col('tag').desc())
    all_tags = (df
                .selectExpr(["event.message_from as user_id", "explode(event.tags) as tag"])
                .groupBy("user_id","tag")
                .agg(F.count("tag"))
                .orderBy(F.col("count(tag)").desc())
                .withColumn("rank", F.row_number().over(window))
                .where(F.col("rank") <= 3)
                .selectExpr(["user_id", "tag", "rank"])
                .groupBy("user_id").pivot("rank") 
                .agg(F.first("tag"))
                .withColumnRenamed('1', 'tag_top_1')  
                .withColumnRenamed('2', 'tag_top_2') 
                .withColumnRenamed('3', 'tag_top_3') 
               )
    
    joined_df = df_top_tags.join(all_tags, 'user_id', 'full')
    filtered_df = (
                    joined_df
                    .filter(joined_df.df_tag_top_1 != joined_df.tag_top_1)|(joined_df.df_tag_top_2 != joined_df.tag_top_2)|(joined_df.df_tag_top_3 != joined_df.tag_top_3)
                    
                  )
#     joined_df.show(10)
    filtered_df.show(10)
#     df.select('event.message_channel_to','event.message_from').show(10)
    
    
    
#             .agg(F.count('message_id').alias('count'))\
#             .orderBy(F.col('count(message_id)').desc())\
#             .withColumn('rating', F.row_number().over(window))\
#             .orderBy(F.col('rating').desc())\
#             .show(10)
#             .where(F.col('rating') <= 3)\
#             .groupBy('user_id')\
#             .pivot('rating')\
#             .agg(F.first('tag'))\
#             .show(10)
                            
    
    return all_tags
    
    
def main():
    spark = SparkSession.builder\
            .master('yarn')\
            .appName('tag_top_user')\
            .getOrCreate()

#     date = '2022-06-04'
#     day_interval = 5
    
    tag_tops('2022-06-04', 5, spark) #.show(10)
#     tag_tops('2022-06-04', 5, spark).repartition(1).write.parquet('/user/masknu11/data/tmp/tag_tops_06_04_5')
#     tag_tops('2022-05-04', 5, spark).repartition(1).write.parquet('/user/masknu11/data/tmp/tag_tops_05_04_5')
#     tag_tops('2022-05-04', 1, spark).repartition(1).write.parquet('/user/masknu11/data/tmp/tag_tops_05_04_1')
    # tag_tops(date, day_interval, spark).show(10)


if __name__ == '__main__':
    main()
