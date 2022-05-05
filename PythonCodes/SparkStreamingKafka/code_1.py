# pip install --force-reinstall pyspark==2.4.6
import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import os, sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

sc = SparkContext(appName='kafka_stream')
ssc = StreamingContext(sc, 2)
msg = KafkaUtils.createDirectStream(ssc,topics=['ex_1'],
                                    kafkaParams={'metadata.broker.list':'localhost:9092'})
words = msg.map(lambda x : x[1]).flatMap(lambda x : x.split(" "))
wordcount = words.map(lambda x : (x,1)).reduceByKey(lambda a,b : a + b)
wordcount.pprint()

ssc.start()
ssc.awaitTermination()