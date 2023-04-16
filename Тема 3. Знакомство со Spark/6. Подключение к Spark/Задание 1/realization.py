from pyspark.sql import SparkSession



spark = (
        SparkSession 
        .builder
        .master('local')
        .config("spark.driver.memory", "1g") 
        .config("spark.driver.cores", 2) 
        .appName("My first session") 
        .getOrCreate()
        )

print(spark)

# VB433DtP5e