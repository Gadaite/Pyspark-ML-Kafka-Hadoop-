#%%
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
conf = SparkConf().setMaster("local").setAppName("app")
sc = SparkContext(conf=conf)
#%%
rdd_1 = sc.textFile("/root/Github_files/python_All/Dataset/T1.txt")
rdd_1.collect()
#%%
words = rdd_1.flatMap(lambda line:line.split(" ")).\
    map(lambda line:(line,1)).\
    reduceByKey(lambda x,y:x+y)
words.collect()


