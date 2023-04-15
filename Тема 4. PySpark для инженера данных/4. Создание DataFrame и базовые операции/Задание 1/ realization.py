import pyspark
from pyspark.sql import SparkSession


spark = SparkSession.builder \
            .master('local') \
            .appName('dataframe_test') \
            .getOrCreate()


data = [
    ('Max', 55),
    ('Yan', 55),
    ('Dmitry', 55),
    ('Ann', 55),
]

columns = ['Name', 'Age']
df = spark.createDataFrame(data=data, schema=columns)