import os, sys
from pyspark.sql import SparkSession, Row

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.appName('app_2')
'''
row_1 = Row("John",35,45000)
row_2 = Row("Sam",31,40000)
row_3 = Row("Tom",38,50000)
data = [row_1, row_2, row_3]
'''

Customer = Row("name", "age")
c1 = Customer("John",45)
c2 = Customer("Sam",48)

data = [c1, c2]
rdd = spark.sparkContext.parallelize(data)
print(rdd.collect())
