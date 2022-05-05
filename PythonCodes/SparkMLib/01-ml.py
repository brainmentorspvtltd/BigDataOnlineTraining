from pyspark.sql import SparkSession
from pyspark.ml.regression import  LinearRegression
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler

spark = SparkSession.builder.appName('linear_regression').getOrCreate()

data = spark.read.csv('headbrain.csv', header=True)
print("Data Schema")
print(data.printSchema())
print("*" * 50)
assembler = VectorAssembler(inputCols=['Head Size(cm^3)'],
                            outputCol='Brain Weight(grams)')

output = assembler.transform(data)
train_data, test_data = output.randomSplit([0.7, 0.3])
# print(train_data.show())
print("Training data describe")
print(train_data.describe.show())
print("*" * 50)
lr = LinearRegression(labelCol='Brain Weight(grams)')
model = lr.fit(train_data,)
print("Model")
print(model)
print("*" * 50)

y_pred = model.evaluate(test_data)
print("Y Prediction")
print(y_pred)
print("*" * 50)