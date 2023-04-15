from datetime import datetime, timedelta
from pyspark.sql import SparkSession

# from pyspark.sql.window import Window 
import pyspark.sql.functions as F
# import pandas as pd
import sys
import os


os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'


def input_paths(date, days_interval, base_input_path):
    # prod_dir = 'masknu11'
    dir_list = list()
    # base_input_path = f'/user/{prod_dir}/data/events'
    
    end_date = datetime.strptime(date, "%Y-%m-%d").date()

    for interval in range(days_interval):
        date = end_date - timedelta(days=interval)
        dir_list.append(f'{base_input_path}/date={date}/event_type=message')   
    return dir_list


def calculation_report(date, days_interval, users_unique_count, base_input_path, verified_tags_path, report_path):
    spark = SparkSession.builder \
                    .master("local") \
                    .appName("Learning DataFrames") \
                    .getOrCreate()
    
    dir_list = input_paths(date, days_interval, base_input_path)
    df = spark.read.parquet(*dir_list)

    df = df.where('event.message_channel_to is not null')\
        .select(F.explode('event.tags').alias('tag'), F.col('event.message_from').alias('user'))\
        .groupBy('tag')\
        .agg(F.countDistinct('user').alias('suggested_count'))\
        .where(F.col('suggested_count') >= users_unique_count)
    
    verified_tags = spark.read.parquet(verified_tags_path)
    candidates = df.join(verified_tags, "tag", "left_anti")
    candidates.describe().show(10, False)

    candidates.select(['tag', 'suggested_count'])\
            .write\
            .mode('append')\
            .parquet(f'{report_path}/date={date}')


def main():
    date = sys.argv[1]
    days_interval = sys.argv[2]
    users_unique_count = sys.argv[3]
    base_input_path = sys.argv[4]
    verified_tags_path = sys.argv[5]
    report_path = sys.argv[6]

    # date = '2022-05-31'
    # days_interval = 5
    # users_unique_count = 300
    # base_input_path = '/user/masknu11/data/events'
    # verified_tags_path = '/user/master/data/snapshots/tags_verified/actual'
    # report_path ='/user/masknu11/data/analytics/verified_tags_candidates_d5'

    calculation_report(date, int(days_interval), int(users_unique_count), base_input_path, verified_tags_path, report_path)



if __name__ == '__main__':
    main()

# spark-submit --master yarn --deploy-mode cluster verified_tags_candidates.py '2022-05-31' 5 300 '/user/masknu11/data/events' '/user/master/data/snapshots/tags_verified/actual' '/user/masknu11/data/analytics/verified_tags_candidates_d5'
# spark-submit --master yarn --deploy-mode cluster verified_tags_candidates.py 2022-05-31 5 300 /user/masknu11/data/events /user/master/data/snapshots/tags_verified/actual /user/masknu11/data/analytics/verified_tags_candidates_d5