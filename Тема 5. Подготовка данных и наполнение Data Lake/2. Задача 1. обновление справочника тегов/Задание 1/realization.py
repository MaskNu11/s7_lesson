# from pyspark import SparkContext, SparkConf
# from pyspark.sql import SparkSession
# from pyspark.sql import SQLContext

# import pyspark.sql.functions as F
from datetime import datetime, timedelta
import pandas as pd
# import os


# os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
# os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
# os.environ['JAVA_HOME']='/usr'
# os.environ['SPARK_HOME'] ='/usr/lib/spark'
# os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8' 
 

def input_paths(date, date_interval):
    # spark = SparkSession.builder \
    #                 .master("local") \
    #                 .appName("Learning DataFrames") \
    #                 .getOrCreate()
    path_list = list()
    prod_dir = 'masknu11'
    base_input_path = f'/user/{prod_dir}/data/events'
    # dateset = {
    #         'verified_tags_candidates_d7': 7,
    #         'verified_tags_candidates_d84': 84,
    #         }
    
    end_date = datetime.strptime(date, "%Y-%m-%d").date()
    # start_date = end_date - timedelta(days=date_interval)

    # date_range = pd.date_range(start=start_date, end=end_date)

#     print(end_date, start_date, date_range)

    # for date in date_range[::-1]:
    for interval in range(date_interval):
        date = end_date - timedelta(days=interval)
        path_list.append(f'{base_input_path}/date={date}/event_type=message')
    return path_list

    # for dataset_name, interval in dateset.items():
    #     base_output_path = f'/user/prod/data/analytics/{dataset_name}/date='

    #     start_date = end_date - timedelta(days=interval)
    #     date_range = pd.date_range(start=start_date, end=end_date)

    #     for date in date_range:
    #         # events = sql.read.json(f'{base_input_path}/date={date}')
    #         print(f'{base_input_path}/date={date.date()}')
    #         df = spark.read.load(f'{base_input_path}/date={date.date()}/')
        # events\
        # .write\
        # .partitionBy('event_type')\
        # .format('parquet')\
        # .save(f'{base_output_path}/date={date}')
            


# def main():
#     date, depth = map(str, input().split())
    

#         # conf = SparkConf().setAppName(f"EventsPartitioningJob-{dataset_name}")
#         # sc = SparkContext(conf=conf)
#         # sql = SQLContext(sc)
#     input_paths(date, int(depth))


# if __name__ == '__main__':
#     main()

# path_list = input_paths('1111-11-01', 2)
# print(path_list)


# rK6YrRAkPV