import os, sys
from pyspark.sql import SparkSession

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

data = [("John", 45000), ("Sam",65000), ("Tom",61000)]

spark = SparkSession.builder.master('local[*]').appName('app_1').getOrCreate()
#spark = SparkSession.builder.appName('app_1').getOrCreate()
rdd = spark.sparkContext.parallelize(data)

df = rdd.toDF()
print(df)
