from pyspark.sql import SparkSession
from datetime import datetime, timedelta

import pandas as pd


def loading_stg(date, date_interval):
    spark = SparkSession.builder \
                        .master("local") \
                        .appName("Learning DataFrames") \
                        .getOrCreate()

    base_input_path = '/user/master/data/events'
    base_output_path = '/user/masknu11/data/events'

    end_date = datetime.strptime(date, "%Y-%m-%d").date()

    start_date = end_date - timedelta(days=date_interval)

    date_range = pd.date_range(start=start_date, end=end_date)


    for date in date_range:
        path = f'{base_input_path}/date={date.date()}/'
        print(path)

        eventsDF = spark.read.json(path)
    #     eventsDF.show(10)
        
        eventsDF\
            .write\
            .partitionBy('event_type')\
            .format('parquet')\
            .mode('append')\
            .save(f'{base_output_path}/date={date.date()}')


def main():
    date = '2022-05-31'
    date_interval = 7

    loading_stg(date, date_interval)

    
if __name__ == '__main__':
    main()