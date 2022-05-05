import os, sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.appName('app_3').getOrCreate()

data = [('Sam','IT','Male',45000),
        ('Tom','SALES','Male',49000),
        ('Jerry','IT','Male',80000),
        ('Jenny','HR','Female',32000),
        ('John','HR','Male',40000),
        ]
columns = ["Name","Department","Gender","Salary"]

df = spark.createDataFrame(data=data,schema=columns)

# withColumn

# change data type
df = df.withColumn("Salary", col("Salary").cast("float"))
print(df.show())
















