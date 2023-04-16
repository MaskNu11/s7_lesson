import sys
import os
 
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F


os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8' 
 

def main():
        date = sys.argv[1]
        base_input_path = sys.argv[2]
        base_output_path = sys.argv[3]

        conf = SparkConf().setAppName(f"EventsPartitioningJob-{date}")
        sc = SparkContext(conf=conf)
        sql = SQLContext(sc)

 # Напишите директорию чтения в общем виде
        events = sql.read.json(f'{base_input_path}/date={date}')

        events.show(10, False)
# Напишите директорию записи
        # events\
        # .write\
        # .partitionBy('event_type')\
        # .format('parquet')\
        # .save(f'{base_output_path}/date={date}')


if __name__ == "__main__":
        main()

# DJhlUTi6G5