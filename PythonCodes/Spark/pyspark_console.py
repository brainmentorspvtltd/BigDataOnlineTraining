Python 3.8.8 (tags/v3.8.8:024d805, Feb 19 2021, 13:18:16) [MSC v.1928 64 bit (AMD64)] on win32
Type "help", "copyright", "credits" or "license()" for more information.
>>> 
================ RESTART: F:/Batches/BigData/pyspark_code_8.py ===============
>>> for item in df.toLocalIterator():
	print(item)

	
Row(Name='Sam', Department='IT', Gender='Male', Salary=45000)
Row(Name='Tom', Department='SALES', Gender='Male', Salary=49000)
Row(Name='Jerry', Department='IT', Gender='Male', Salary=80000)
Row(Name='Jenny', Department='HR', Gender='Female', Salary=32000)
Row(Name='John', Department='HR', Gender='Male', Salary=40000)
>>> ----------------------------------------
Exception happened during processing of request from ('127.0.0.1', 58425)
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

================ RESTART: F:/Batches/BigData/pyspark_code_8.py ===============
+-----+----------+------+------+
| Name|Department|Gender|Salary|
+-----+----------+------+------+
|  Sam|        IT|  Male| 45000|
|  Tom|     SALES|  Male| 49000|
|Jerry|        IT|  Male| 80000|
|Jenny|        HR|Female| 32000|
| John|        HR|  Male| 40000|
+-----+----------+------+------+

None
>>> df.withColumn("Salary", col("Salary").cast("float")).show()
+-----+----------+------+-------+
| Name|Department|Gender| Salary|
+-----+----------+------+-------+
|  Sam|        IT|  Male|45000.0|
|  Tom|     SALES|  Male|49000.0|
|Jerry|        IT|  Male|80000.0|
|Jenny|        HR|Female|32000.0|
| John|        HR|  Male|40000.0|
+-----+----------+------+-------+

>>> df.withColumn("Salary", col("Salary") + 2000).show()
+-----+----------+------+------+
| Name|Department|Gender|Salary|
+-----+----------+------+------+
|  Sam|        IT|  Male| 47000|
|  Tom|     SALES|  Male| 51000|
|Jerry|        IT|  Male| 82000|
|Jenny|        HR|Female| 34000|
| John|        HR|  Male| 42000|
+-----+----------+------+------+

>>> df.withColumn("City", lit("New York")).show()
+-----+----------+------+------+--------+
| Name|Department|Gender|Salary|    City|
+-----+----------+------+------+--------+
|  Sam|        IT|  Male| 45000|New York|
|  Tom|     SALES|  Male| 49000|New York|
|Jerry|        IT|  Male| 80000|New York|
|Jenny|        HR|Female| 32000|New York|
| John|        HR|  Male| 40000|New York|
+-----+----------+------+------+--------+

>>> df.withColumnRenamed("Name","Emp Name").show()
+--------+----------+------+------+
|Emp Name|Department|Gender|Salary|
+--------+----------+------+------+
|     Sam|        IT|  Male| 45000|
|     Tom|     SALES|  Male| 49000|
|   Jerry|        IT|  Male| 80000|
|   Jenny|        HR|Female| 32000|
|    John|        HR|  Male| 40000|
+--------+----------+------+------+

>>> #df.drop("col_name").show()
>>> ----------------------------------------
Exception happened during processing of request from ('127.0.0.1', 58489)
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

================ RESTART: F:/Batches/BigData/pyspark_code_9.py ===============
+-----+----------+------+------+
| Name|Department|Gender|Salary|
+-----+----------+------+------+
|Jerry|        IT|  Male| 80000|
| John|        HR|  Male| 40000|
|  Tom|     SALES|  Male| 49000|
|  Sam|        IT|  Male| 45000|
|Jenny|        HR|Female| 32000|
+-----+----------+------+------+

>>> df.show()
+-----+----------+------+------+
| Name|Department|Gender|Salary|
+-----+----------+------+------+
|  Sam|        IT|  Male| 45000|
|  Tom|     SALES|  Male| 49000|
|Jerry|        IT|  Male| 80000|
|Jenny|        HR|Female| 32000|
| John|        HR|  Male| 40000|
|  Tom|     SALES|  Male| 49000|
|Jerry|        IT|  Male| 80000|
|Jenny|        HR|Female| 32000|
|Jerry|        IT|  Male| 80000|
+-----+----------+------+------+

>>> df.dropDuplicates(["Name"]).show()
+-----+----------+------+------+
| Name|Department|Gender|Salary|
+-----+----------+------+------+
|  Tom|     SALES|  Male| 49000|
|Jerry|        IT|  Male| 80000|
|Jenny|        HR|Female| 32000|
| John|        HR|  Male| 40000|
|  Sam|        IT|  Male| 45000|
+-----+----------+------+------+

>>> df.dropDuplicates(["Department"]).show()
+-----+----------+------+------+
| Name|Department|Gender|Salary|
+-----+----------+------+------+
|Jenny|        HR|Female| 32000|
|  Tom|     SALES|  Male| 49000|
|  Sam|        IT|  Male| 45000|
+-----+----------+------+------+

>>> ----------------------------------------
Exception happened during processing of request from ('127.0.0.1', 58619)
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

=============== RESTART: F:/Batches/BigData/pyspark_code_10.py ===============
+----------+-----------+
|Department|sum(Salary)|
+----------+-----------+
|        HR|     104000|
|     SALES|      98000|
|        IT|     285000|
+----------+-----------+

>>> df.groupby("Department").max("Salary").show()
+----------+-----------+
|Department|max(Salary)|
+----------+-----------+
|        HR|      40000|
|     SALES|      49000|
|        IT|      80000|
+----------+-----------+

>>> df.groupby("Department").median("Salary").show()
Traceback (most recent call last):
  File "<pyshell#12>", line 1, in <module>
    df.groupby("Department").median("Salary").show()
AttributeError: 'GroupedData' object has no attribute 'median'
>>> df.groupby("Department").mean("Salary").show()
+----------+------------------+
|Department|       avg(Salary)|
+----------+------------------+
|        HR|34666.666666666664|
|     SALES|           49000.0|
|        IT|           71250.0|
+----------+------------------+

>>> df.groupby("Department").agg(mean("Salary").alias("Avg Salary"), sum("Salary").alias("Total Salary")).show()
Traceback (most recent call last):
  File "<pyshell#14>", line 1, in <module>
    df.groupby("Department").agg(mean("Salary").alias("Avg Salary"), sum("Salary").alias("Total Salary")).show()
NameError: name 'mean' is not defined
>>> from pyspark.sql.functions import sum, avg, max, min
>>> df.groupby("Department").agg(avg("Salary").alias("Avg Salary"), sum("Salary").alias("Total Salary")).show()
+----------+------------------+------------+
|Department|        Avg Salary|Total Salary|
+----------+------------------+------------+
|        HR|34666.666666666664|      104000|
|     SALES|           49000.0|       98000|
|        IT|           71250.0|      285000|
+----------+------------------+------------+

>>> ----------------------------------------
Exception happened during processing of request from ('127.0.0.1', 58705)
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

=============== RESTART: F:/Batches/BigData/pyspark_code_11.py ===============
+-----+-----+------+-----+
| Name|   HR|    IT|SALES|
+-----+-----+------+-----+
|  Tom| null|  null|98000|
|Jerry| null|240000| null|
|Jenny|64000|  null| null|
| John|40000|  null| null|
|  Sam| null| 45000| null|
+-----+-----+------+-----+

>>> df.groupBy("Name").pivot("Department").sum("Salary").show()
+-----+-----+------+-----+
| Name|   HR|    IT|SALES|
+-----+-----+------+-----+
|  Tom| null|  null|98000|
|Jerry| null|240000| null|
|Jenny|64000|  null| null|
| John|40000|  null| null|
|  Sam| null| 45000| null|
+-----+-----+------+-----+

>>> df_2 = df.groupBy("Name").pivot("Department").sum("Salary")
>>> df_2.na.fill(value=0).show()
+-----+-----+------+-----+
| Name|   HR|    IT|SALES|
+-----+-----+------+-----+
|  Tom|    0|     0|98000|
|Jerry|    0|240000|    0|
|Jenny|64000|     0|    0|
| John|40000|     0|    0|
|  Sam|    0| 45000|    0|
+-----+-----+------+-----+

>>> df_2.select("HR")
DataFrame[HR: bigint]
>>> df_2.select("HR").mean()
Traceback (most recent call last):
  File "<pyshell#21>", line 1, in <module>
    df_2.select("HR").mean()
  File "C:\Python38\lib\site-packages\pyspark\sql\dataframe.py", line 1643, in __getattr__
    raise AttributeError(
AttributeError: 'DataFrame' object has no attribute 'mean'
>>> df_2.select("HR").avg()
Traceback (most recent call last):
  File "<pyshell#22>", line 1, in <module>
    df_2.select("HR").avg()
  File "C:\Python38\lib\site-packages\pyspark\sql\dataframe.py", line 1643, in __getattr__
    raise AttributeError(
AttributeError: 'DataFrame' object has no attribute 'avg'
>>> x = df_2.select("HR")
>>> from pyspark.sql.functions import mean
>>> mean
<function mean at 0x00000289FCEF7AF0>
>>> mean(df_2['HR'])
Column<'avg(HR)'>
>>> df_2['HR']['mean']
Column<'HR[mean]'>
>>> df_2['HR']['mean'].show()
Traceback (most recent call last):
  File "<pyshell#28>", line 1, in <module>
    df_2['HR']['mean'].show()
TypeError: 'Column' object is not callable
>>> df_2['HR']['mean'].show
Column<'HR[mean][show]'>
>>> print(df_2['HR']['mean'])
Column<'HR[mean]'>
>>> df_2.na.fill(value=df_2['HR']['mean']).show()
Traceback (most recent call last):
  File "<pyshell#31>", line 1, in <module>
    df_2.na.fill(value=df_2['HR']['mean']).show()
  File "C:\Python38\lib\site-packages\pyspark\sql\dataframe.py", line 2703, in fill
    return self.df.fillna(value=value, subset=subset)
  File "C:\Python38\lib\site-packages\pyspark\sql\dataframe.py", line 2068, in fillna
    raise ValueError("value should be a float, int, string, bool or dict")
ValueError: value should be a float, int, string, bool or dict
>>> df_2.show()
+-----+-----+------+-----+
| Name|   HR|    IT|SALES|
+-----+-----+------+-----+
|  Tom| null|  null|98000|
|Jerry| null|240000| null|
|Jenny|64000|  null| null|
| John|40000|  null| null|
|  Sam| null| 45000| null|
+-----+-----+------+-----+

>>> def fill_mean(df, exclude=set()):
	stats = df.agg(*(avg(col) for col in df.columns if col not in exclude))
	return df.na.fill(stats.first().asDict())

>>> fill_mean(df_2, ["Name"])
Traceback (most recent call last):
  File "<pyshell#37>", line 1, in <module>
    fill_mean(df_2, ["Name"])
  File "<pyshell#36>", line 2, in fill_mean
    stats = df.agg(*(avg(col) for col in df.columns if col not in exclude))
  File "<pyshell#36>", line 2, in <genexpr>
    stats = df.agg(*(avg(col) for col in df.columns if col not in exclude))
NameError: name 'avg' is not defined
>>> from pyspark.sql.functions import mean, avg
>>> def fill_mean(df, exclude=set()):
	stats = df.agg(*(avg(col) for col in df.columns if col not in exclude))
	return df.na.fill(stats.first().asDict())

>>> fill_mean(df_2, ["Name"])
Traceback (most recent call last):
  File "<pyshell#41>", line 1, in <module>
    fill_mean(df_2, ["Name"])
  File "<pyshell#40>", line 3, in fill_mean
    return df.na.fill(stats.first().asDict())
  File "C:\Python38\lib\site-packages\pyspark\sql\dataframe.py", line 2703, in fill
    return self.df.fillna(value=value, subset=subset)
  File "C:\Python38\lib\site-packages\pyspark\sql\dataframe.py", line 2077, in fillna
    return DataFrame(self._jdf.na().fill(value), self.sql_ctx)
  File "C:\Python38\lib\site-packages\py4j\java_gateway.py", line 1304, in __call__
    return_value = get_return_value(
  File "C:\Python38\lib\site-packages\pyspark\sql\utils.py", line 117, in deco
    raise converted from None
pyspark.sql.utils.AnalysisException: Cannot resolve column name "avg(SALES)" among (Name, HR, IT, SALES)
>>> def fill_mean(df, exclude=set()):
	stats = df.agg(*(avg(col).alias(col) for col in df.columns if col not in exclude))
	return df.na.fill(stats.first().asDict())

>>> fill_mean(df_2, ["Name"])
DataFrame[Name: string, HR: bigint, IT: bigint, SALES: bigint]
>>> df_2.show()
+-----+-----+------+-----+
| Name|   HR|    IT|SALES|
+-----+-----+------+-----+
|  Tom| null|  null|98000|
|Jerry| null|240000| null|
|Jenny|64000|  null| null|
| John|40000|  null| null|
|  Sam| null| 45000| null|
+-----+-----+------+-----+

>>> fill_mean(df_2, ["Name"]).show()
+-----+-----+------+-----+
| Name|   HR|    IT|SALES|
+-----+-----+------+-----+
|  Tom|52000|142500|98000|
|Jerry|52000|240000|98000|
|Jenny|64000|142500|98000|
| John|40000|142500|98000|
|  Sam|52000| 45000|98000|
+-----+-----+------+-----+

>>> 