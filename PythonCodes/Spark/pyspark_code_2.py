Python 3.8.8 (tags/v3.8.8:024d805, Feb 19 2021, 13:18:16) [MSC v.1928 64 bit (AMD64)] on win32
Type "help", "copyright", "credits" or "license()" for more information.
>>> import os, sys
>>> import pyspark
>>> os.environ['PYSPARK_PYTHON'] = sys.executable
>>> os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
>>> sc = pyspark.SparkContext("local[*]")
>>> words = sc.parallelize(["hi","hello","hi","hey","hey","hi"])
>>> words
ParallelCollectionRDD[0] at readRDDFromFile at PythonRDD.scala:274
>>> words.count()
6
>>> words.collect()
['hi', 'hello', 'hi', 'hey', 'hey', 'hi']
>>> def func(x):
	print(x)

	
>>> words.foreach(func)
>>> def func(x):
	print(x.startswith('he'))

	
>>> words.foreach(func)
>>> f = words.foreach(func)
>>> words
ParallelCollectionRDD[0] at readRDDFromFile at PythonRDD.scala:274
>>> words.foreach(func)

>>> def func(x):
	print(x)

	
>>> words.foreach(func)
>>> def f(x):print(x)

>>> words.foreach(f)
>>> words.collect()
['hi', 'hello', 'hi', 'hey', 'hey', 'hi']
>>> f = sc.textFile('F:\count_vehciles.txt')
>>> f
F:\count_vehciles.txt MapPartitionsRDD[9] at textFile at NativeMethodAccessorImpl.java:0
>>> f.count()
1
>>> f.collect()
['car,bike,car,car,bike,bus,bus,truck,bus,truck,truck,bus,car,bike,bike,bike,bike,truck,bus']
>>> map(lambda x : x.split(','), f.collect())
<map object at 0x000002595065FA30>
>>> list(map(lambda x : x.split(','), f.collect()))
[['car', 'bike', 'car', 'car', 'bike', 'bus', 'bus', 'truck', 'bus', 'truck', 'truck', 'bus', 'car', 'bike', 'bike', 'bike', 'bike', 'truck', 'bus']]
>>> list(map(lambda x : x.split(','), f))
Traceback (most recent call last):
  File "<pyshell#32>", line 1, in <module>
    list(map(lambda x : x.split(','), f))
TypeError: 'RDD' object is not iterable
>>> words
ParallelCollectionRDD[0] at readRDDFromFile at PythonRDD.scala:274
>>> f.map(lambda x : x.split(','))
PythonRDD[11] at RDD at PythonRDD.scala:53
>>> f.map(lambda x : x.split(',')).collect()
[['car', 'bike', 'car', 'car', 'bike', 'bus', 'bus', 'truck', 'bus', 'truck', 'truck', 'bus', 'car', 'bike', 'bike', 'bike', 'bike', 'truck', 'bus']]
>>> words.filter(lambda x : 'i' in x).collect()
['hi', 'hi', 'hi']
>>> arr = sc.parallelize([2,4,6,8,10])
>>> arr.reduce(add)
Traceback (most recent call last):
  File "<pyshell#38>", line 1, in <module>
    arr.reduce(add)
NameError: name 'add' is not defined
>>> from operator import add
>>> add
<built-in function add>
>>> arr.reduce(add)
30
>>> from pyspark import SparkContext
>>> sc_1 = SparkContext("local", "App_1")
Traceback (most recent call last):
  File "<pyshell#43>", line 1, in <module>
    sc_1 = SparkContext("local", "App_1")
  File "C:\Python38\lib\site-packages\pyspark\context.py", line 144, in __init__
    SparkContext._ensure_initialized(self, gateway=gateway, conf=conf)
  File "C:\Python38\lib\site-packages\pyspark\context.py", line 342, in _ensure_initialized
    raise ValueError(
ValueError: Cannot run multiple SparkContexts at once; existing SparkContext(app=pyspark-shell, master=local[*]) created by __init__ at <pyshell#4>:1 
>>> words.getNumPartitions()
4
>>> 