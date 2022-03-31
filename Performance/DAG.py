from pyspark import SparkConf,SparkContext
import time
conf = SparkConf()
sc = SparkContext(conf=conf,appName="DAG",master="local[*]").getOrCreate()
sc.setLogLevel("ERROR")
rdd = sc.parallelize([i for i in range(0,4)])
rdd1 = rdd.repartition(8)
rdd1.foreach(print)
print(rdd.getNumPartitions())
time.sleep(60)