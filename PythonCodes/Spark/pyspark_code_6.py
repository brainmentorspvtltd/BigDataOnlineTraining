import os, sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.appName('app_3').getOrCreate()

colObj = lit("Column_1")

data = [("Sam",45),("John",28),("Tom",45),("Jack",18),("Peter",27)]
df = spark.createDataFrame(data).toDF("name","age")
print(df.select(col("name")).show())
