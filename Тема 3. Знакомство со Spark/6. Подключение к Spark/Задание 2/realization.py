from pyspark.sql import SparkSession



spark = SparkSession \
        .builder \
        .master("yarn")\
        .config("spark.driver.memory", "1g") \
        .config("spark.driver.cores", 2) \
        .appName("My second session") \
        .getOrCreate()

print(spark)