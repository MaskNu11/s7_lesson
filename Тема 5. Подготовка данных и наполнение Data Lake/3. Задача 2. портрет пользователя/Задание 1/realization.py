import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

import findspark
findspark.init()
findspark.find()




from datetime import datetime, timedelta
from pyspark.sql.window import Window 
from pyspark.sql import SparkSession

import pyspark.sql.functions as F


def input_path(date, day_interval, base_input_path):
    end_date = datetime.strptime(date, '%Y-%m-%d').date()

    rows = [f'{base_input_path}/date={end_date - timedelta(days=interval)}/event_type=message' for interval in range(day_interval)]
    return rows


def tag_tops(date, day_interval, spark):
    base_input_path = '/user/masknu11/data/events'
    
    dir_list = input_path(date, day_interval, base_input_path)

    df = spark.read.parquet(*dir_list)
    
#     df.printSchema()

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

    return all_tags
    
    
def main():
    spark = SparkSession.builder\
            .master('yarn')\
            .appName('tag_top_user')\
            .getOrCreate()

#     date = '2022-06-04'
#     day_interval = 5
    
    tag_tops('2022-06-04', 5, spark).show(10)
#     tag_tops('2022-06-04', 5, spark).repartition(1).write.parquet('/user/masknu11/data/tmp/tag_tops_06_04_5')
#     tag_tops('2022-05-04', 5, spark).repartition(1).write.parquet('/user/masknu11/data/tmp/tag_tops_05_04_5')
#     tag_tops('2022-05-04', 1, spark).repartition(1).write.parquet('/user/masknu11/data/tmp/tag_tops_05_04_1')
    # tag_tops(date, day_interval, spark).show(10)


if __name__ == '__main__':
    main()
