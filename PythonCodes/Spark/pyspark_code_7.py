Python 3.8.8 (tags/v3.8.8:024d805, Feb 19 2021, 13:18:16) [MSC v.1928 64 bit (AMD64)] on win32
Type "help", "copyright", "credits" or "license()" for more information.
>>> 
================= RESTART: F:/Batches/BigData/pyspark_code_5.py ================
[<Row('John', 35, 45000)>, <Row('Sam', 31, 40000)>, <Row('Tom', 38, 50000)>]
>>> 
================ RESTART: F:/Batches/BigData/pyspark_code_5.py ===============
[Row(name='John', age=45), Row(name='Sam', age=48)]
>>> r1 = Row(name = "Sam", age = 20)
>>> r1
Row(name='Sam', age=20)
>>> r1.name
'Sam'
>>> r1.age
20
>>> c1.name
'John'
>>> c2.name
'Sam'
>>> df = spark.createDataFrame(data)
>>> df.show()
+----+---+
|name|age|
+----+---+
|John| 45|
| Sam| 48|
+----+---+

>>> ----------------------------------------
Exception happened during processing of request from ('127.0.0.1', 54087)
Traceback (most recent call last):
  File "C:\Python38\lib\socketserver.py", line 316, in _handle_request_noblock
    self.process_request(request, client_address)
  File "C:\Python38\lib\socketserver.py", line 347, in process_request
    self.finish_request(request, client_address)
  File "C:\Python38\lib\socketserver.py", line 360, in finish_request
    self.RequestHandlerClass(request, client_address, self)
  File "C:\Python38\lib\socketserver.py", line 720, in __init__
    self.handle()
  File "C:\Python38\lib\site-packages\pyspark\accumulators.py", line 262, in handle
    poll(accum_updates)
  File "C:\Python38\lib\site-packages\pyspark\accumulators.py", line 235, in poll
    if func():
  File "C:\Python38\lib\site-packages\pyspark\accumulators.py", line 239, in accum_updates
    num_updates = read_int(self.rfile)
  File "C:\Python38\lib\site-packages\pyspark\serializers.py", line 562, in read_int
    length = stream.read(4)
  File "C:\Python38\lib\socket.py", line 669, in readinto
    return self._sock.recv_into(b)
ConnectionResetError: [WinError 10054] An existing connection was forcibly closed by the remote host
----------------------------------------

================ RESTART: F:/Batches/BigData/pyspark_code_6.py ===============
>>> df
DataFrame[name: string, age: bigint]
>>> df.show()
+----+---+
|name|age|
+----+---+
| Sam| 45|
|John| 28|
+----+---+

>>> df.select(df.name)
DataFrame[name: string]
>>> df.select(df.name).show()
+----+
|name|
+----+
| Sam|
|John|
+----+

>>> df.select(df['name']).show()
+----+
|name|
+----+
| Sam|
|John|
+----+

>>> from pyspark.sql.functions import col
>>> df.select(col("name")).show()
+----+
|name|
+----+
| Sam|
|John|
+----+

>>> data = [(34,3,6),(12,45,68),(44,32,8)]
>>> df = spark.createDataFrame(data).toDF("col1","col2","col3")
>>> df.select(df.col1 + df.col2).show()
+-------------+
|(col1 + col2)|
+-------------+
|           37|
|           57|
|           76|
+-------------+

>>> df.select(df.col1 - df.col2).show()
+-------------+
|(col1 - col2)|
+-------------+
|           31|
|          -33|
|           12|
+-------------+

>>> df.select(df.col1 / df.col2).show()
+-------------------+
|      (col1 / col2)|
+-------------------+
| 11.333333333333334|
|0.26666666666666666|
|              1.375|
+-------------------+

>>> df.select(df.col1 > df.col2).show()
+-------------+
|(col1 > col2)|
+-------------+
|         true|
|        false|
|         true|
+-------------+

>>> df.sort(df.col1.asc()).show()
+----+----+----+
|col1|col2|col3|
+----+----+----+
|  12|  45|  68|
|  34|   3|   6|
|  44|  32|   8|
+----+----+----+

>>> df.select(df.col1.cast("float"))
DataFrame[col1: float]
>>> df.select(df.col1.cast("float")).show()
+----+
|col1|
+----+
|34.0|
|12.0|
|44.0|
+----+

>>> df.filter(df.col1.between(30,50)).show()
+----+----+----+
|col1|col2|col3|
+----+----+----+
|  34|   3|   6|
|  44|  32|   8|
+----+----+----+

>>> 
================ RESTART: F:/Batches/BigData/pyspark_code_6.py ===============
+-----+
| name|
+-----+
|  Sam|
| John|
|  Tom|
| Jack|
|Peter|
+-----+

None
>>> df
DataFrame[name: string, age: bigint]
>>> df.select(df.name.like("%o")).show()
+------------+
|name LIKE %o|
+------------+
|       false|
|       false|
|       false|
|       false|
|       false|
+------------+

>>> df.select(df.name.like("%o%")).show()
+-------------+
|name LIKE %o%|
+-------------+
|        false|
|         true|
|         true|
|        false|
|        false|
+-------------+

>>> df.select(df.name.like("o%")).show()
+------------+
|name LIKE o%|
+------------+
|       false|
|       false|
|       false|
|       false|
|       false|
+------------+

>>> df.filter(df.name.like("%o%")).show()
+----+---+
|name|age|
+----+---+
|John| 28|
| Tom| 45|
+----+---+

>>> from pyspark.sql.functions import when
>>> df.select(df.name, df.age, when(df.age >= 30)).show()
Traceback (most recent call last):
  File "<pyshell#31>", line 1, in <module>
    df.select(df.name, df.age, when(df.age >= 30)).show()
TypeError: when() missing 1 required positional argument: 'value'
>>> df.select(df.name, df.age, when(df.age >= 30, "Yes")).show()
+-----+---+----------------------------------+
| name|age|CASE WHEN (age >= 30) THEN Yes END|
+-----+---+----------------------------------+
|  Sam| 45|                               Yes|
| John| 28|                              null|
|  Tom| 45|                               Yes|
| Jack| 18|                              null|
|Peter| 27|                              null|
+-----+---+----------------------------------+

>>> df.select(df.name, df.age, when(df.age >= 30, "Yes").otherwise("No").alias("Condition")).show()
+-----+---+---------+
| name|age|Condition|
+-----+---+---------+
|  Sam| 45|      Yes|
| John| 28|       No|
|  Tom| 45|      Yes|
| Jack| 18|       No|
|Peter| 27|       No|
+-----+---+---------+

>>> 