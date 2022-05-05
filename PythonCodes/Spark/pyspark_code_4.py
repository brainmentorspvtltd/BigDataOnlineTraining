Python 3.8.8 (tags/v3.8.8:024d805, Feb 19 2021, 13:18:16) [MSC v.1928 64 bit (AMD64)] on win32
Type "help", "copyright", "credits" or "license()" for more information.
>>> import os, sys
>>> from pyspark import SparkContext
>>> 
================ RESTART: F:/Batches/BigData/pyspark_code_3.py ===============
Traceback (most recent call last):
  File "F:/Batches/BigData/pyspark_code_3.py", line 2, in <module>
    from pyspark import SparkContext, SparkSession
ImportError: cannot import name 'SparkSession' from 'pyspark' (C:\Python38\lib\site-packages\pyspark\__init__.py)
>>> 
================ RESTART: F:/Batches/BigData/pyspark_code_3.py ===============
>>> rdd
ParallelCollectionRDD[0] at readRDDFromFile at PythonRDD.scala:274
>>> 
================ RESTART: F:/Batches/BigData/pyspark_code_3.py ===============
DataFrame[_1: string, _2: bigint]
>>> df.printSchema()
root
 |-- _1: string (nullable = true)
 |-- _2: long (nullable = true)

>>> columns = ["name","salary"]
>>> df = rdd.toDF()
>>> df.printSchema()
root
 |-- _1: string (nullable = true)
 |-- _2: long (nullable = true)

>>> df = rdd.toDF(columns)
>>> df.printSchema()
root
 |-- name: string (nullable = true)
 |-- salary: long (nullable = true)

>>> df.collect()
[Row(name='John', salary=45000), Row(name='Sam', salary=65000), Row(name='Tom', salary=61000)]
>>> df_2 = spark.createDataFrame(rdd).toDF(*columns)
>>> df_2.printSchema()
root
 |-- name: string (nullable = true)
 |-- salary: long (nullable = true)

>>> df_2.collect()
[Row(name='John', salary=45000), Row(name='Sam', salary=65000), Row(name='Tom', salary=61000)]
>>> df_2.show()
+----+------+
|name|salary|
+----+------+
|John| 45000|
| Sam| 65000|
| Tom| 61000|
+----+------+

>>> df = spark.read.csv('F:/Datasets/home_data.csv')
>>> df.head()
Row(_c0='id', _c1='date', _c2='price', _c3='bedrooms', _c4='bathrooms', _c5='sqft_living', _c6='sqft_lot', _c7='floors', _c8='waterfront', _c9='view', _c10='condition', _c11='grade', _c12='sqft_above', _c13='sqft_basement', _c14='yr_built', _c15='yr_renovated', _c16='zipcode', _c17='lat', _c18='long', _c19='sqft_living15', _c20='sqft_lot15')
>>> df.head(4)
[Row(_c0='id', _c1='date', _c2='price', _c3='bedrooms', _c4='bathrooms', _c5='sqft_living', _c6='sqft_lot', _c7='floors', _c8='waterfront', _c9='view', _c10='condition', _c11='grade', _c12='sqft_above', _c13='sqft_basement', _c14='yr_built', _c15='yr_renovated', _c16='zipcode', _c17='lat', _c18='long', _c19='sqft_living15', _c20='sqft_lot15'), Row(_c0='7129300520', _c1='20141013T000000', _c2='221900', _c3='3', _c4='1', _c5='1180', _c6='5650', _c7='1', _c8='0', _c9='0', _c10='3', _c11='7', _c12='1180', _c13='0', _c14='1955', _c15='0', _c16='98178', _c17='47.5112', _c18='-122.257', _c19='1340', _c20='5650'), Row(_c0='6414100192', _c1='20141209T000000', _c2='538000', _c3='3', _c4='2.25', _c5='2570', _c6='7242', _c7='2', _c8='0', _c9='0', _c10='3', _c11='7', _c12='2170', _c13='400', _c14='1951', _c15='1991', _c16='98125', _c17='47.721', _c18='-122.319', _c19='1690', _c20='7639'), Row(_c0='5631500400', _c1='20150225T000000', _c2='180000', _c3='2', _c4='1', _c5='770', _c6='10000', _c7='1', _c8='0', _c9='0', _c10='3', _c11='6', _c12='770', _c13='0', _c14='1933', _c15='0', _c16='98028', _c17='47.7379', _c18='-122.233', _c19='2720', _c20='8062')]
>>> df.show(5)
+----------+---------------+------+--------+---------+-----------+--------+------+----------+----+---------+-----+----------+-------------+--------+------------+-------+-------+--------+-------------+----------+
|       _c0|            _c1|   _c2|     _c3|      _c4|        _c5|     _c6|   _c7|       _c8| _c9|     _c10| _c11|      _c12|         _c13|    _c14|        _c15|   _c16|   _c17|    _c18|         _c19|      _c20|
+----------+---------------+------+--------+---------+-----------+--------+------+----------+----+---------+-----+----------+-------------+--------+------------+-------+-------+--------+-------------+----------+
|        id|           date| price|bedrooms|bathrooms|sqft_living|sqft_lot|floors|waterfront|view|condition|grade|sqft_above|sqft_basement|yr_built|yr_renovated|zipcode|    lat|    long|sqft_living15|sqft_lot15|
|7129300520|20141013T000000|221900|       3|        1|       1180|    5650|     1|         0|   0|        3|    7|      1180|            0|    1955|           0|  98178|47.5112|-122.257|         1340|      5650|
|6414100192|20141209T000000|538000|       3|     2.25|       2570|    7242|     2|         0|   0|        3|    7|      2170|          400|    1951|        1991|  98125| 47.721|-122.319|         1690|      7639|
|5631500400|20150225T000000|180000|       2|        1|        770|   10000|     1|         0|   0|        3|    6|       770|            0|    1933|           0|  98028|47.7379|-122.233|         2720|      8062|
|2487200875|20141209T000000|604000|       4|        3|       1960|    5000|     1|         0|   0|        5|    7|      1050|          910|    1965|           0|  98136|47.5208|-122.393|         1360|      5000|
+----------+---------------+------+--------+---------+-----------+--------+------+----------+----+---------+-----+----------+-------------+--------+------------+-------+-------+--------+-------------+----------+
only showing top 5 rows

>>> df = spark.read.csv('F:/Datasets/states_daily.json')
>>> df.show(3)
+------------------+
|               _c0|
+------------------+
|                 {|
|	"states_daily": [|
|               		{|
+------------------+
only showing top 3 rows

>>> df = spark.read.json('F:/Datasets/states_daily.json')
>>> df.show(3)
Traceback (most recent call last):
  File "<pyshell#21>", line 1, in <module>
    df.show(3)
  File "C:\Python38\lib\site-packages\pyspark\sql\dataframe.py", line 484, in show
    print(self._jdf.showString(n, 20, vertical))
  File "C:\Python38\lib\site-packages\py4j\java_gateway.py", line 1304, in __call__
    return_value = get_return_value(
  File "C:\Python38\lib\site-packages\pyspark\sql\utils.py", line 117, in deco
    raise converted from None
pyspark.sql.utils.AnalysisException: Since Spark 2.3, the queries from raw JSON/CSV files are disallowed when the
referenced columns only include the internal corrupt record column
(named _corrupt_record by default). For example:
spark.read.schema(schema).json(file).filter($"_corrupt_record".isNotNull).count()
and spark.read.schema(schema).json(file).select("_corrupt_record").show().
Instead, you can cache or save the parsed results and then send the same query.
For example, val df = spark.read.schema(schema).json(file).cache() and then
df.filter($"_corrupt_record".isNotNull).count().
>>> 