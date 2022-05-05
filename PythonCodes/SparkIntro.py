import pyspark
from pyspark.sql import SparkSession
import os, sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.master('local').appName('App_1').getOrCreate()
cols = ["names", "scores"]
dataset = [("Sachin", 57),("Dravid",120),("Virat",36),("Dhoni",44)]

rdd = spark.sparkContext.parallelize(dataset)
df_1 = rdd.toDF()
# print(df_1.printSchema())
df_2 = spark.createDataFrame(rdd).toDF(*cols)
print(df_2.show())
