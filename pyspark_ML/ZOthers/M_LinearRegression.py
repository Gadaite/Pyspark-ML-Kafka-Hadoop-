#%%
from pyspark import SparkConf,SparkContext
conf = SparkConf().setAppName("app").setMaster("local")
sc = SparkContext(conf=conf)
#%%
# 导入数据集
rdd = sc.textFile("/root/Github_files/spark_object/src/main/resources/DataSet_sparklearn/D06/lpsa.data")
rdd.collect()
#%%
import pyspark.mllib.regression.LabeledPoint
rdd1 = rdd.map(lambda line :line.split(","),LabelPoint)