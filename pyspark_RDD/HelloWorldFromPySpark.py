#%%
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
conf = SparkConf().setMaster("local[*]").setAppName("APP")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.appName("APP").master("local[*]").getOrCreate()
#%%
list_a =["hadoop","scala","java","spark","sql","java"]
rdd_a = sc.parallelize(list_a)
rdd_a.collect()
#%%
sorted(rdd_a.map(lambda x:x.upper()).collect())
#%%
list_b = [[1,3,5],[2,4,6],[8,10,9]]
rdd_b = sc.parallelize(list_b)
rdd_b.collect()
#%%
rdd_b.map(lambda x:[x[0]**2,x[1]**2,x[2]**2]).collect()
#%%
rdd_b.map(lambda x:x.split(" ")).collect()
#%%
rdd_c = sc.textFile("/root/Github_files/python_All/Dataset/T1.txt")
rdd_c.collect()
rdd_c.map(lambda x:x.split(" ")).collect()
#%%
# rdd_c.map(lambda x:x.split(" ")).map(lambda y:y.upper(),x.split(" ")).collect()
#%%
spark.stop()
