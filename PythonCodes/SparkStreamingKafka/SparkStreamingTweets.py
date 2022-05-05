# pip install --force-reinstall pyspark==2.4.6
import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
import os, sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

sc = SparkContext(appName='twitter_stream')
ssc = StreamingContext(sc, 5)

sqlContext = SQLContext(sc)
socket_stream = ssc.socketTextStream('127.0.0.1', 5555)

lines = socket_stream.window(20)

words = lines.map(lambda x : x[1]).flatMap(lambda x : x.split(" "))
wordcount = words.map(lambda x : (x,1)).reduceByKey(lambda a,b : a + b)
wordcount.pprint()

ssc.start()
ssc.awaitTermination()