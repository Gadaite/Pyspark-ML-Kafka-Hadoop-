#%%

from pyspark import SparkConf,SparkContext
conf = SparkConf().setMaster("local").setAppName("app")
sc = SparkContext(conf=conf)
#%%
rdd1 = sc.parallelize([1,2,3,4])
rdd2 = sc.parallelize([3,4,7,9])
#%%交集
rdd3 = rdd1.intersection(rdd2)
rdd3.collect()
#%%并集
rdd4 = rdd1.union(rdd2)
rdd4.collect()
#%%差集
rdd5 = rdd1.subtract(rdd2)
rdd5.collect()
#%%拉链
rdd6 = rdd1.zip(rdd2)
rdd6.collect()
#%%
listA = [1,2,3,4,5,6]
",".join(str(i) for i in listA)

#%%
"""
zip数据类型可以不一致，交并补必须一致
zip需要按照分区数量进行zip操作,数量也必须一致
"""
#%%
rdd_A = sc.parallelize(["A","B"],2)
rdd_B = sc.parallelize([1,2],2)
rdd_C = rdd_A.zip(rdd_B)
rdd_C.collect()

#%%:key-value
rdd7 = rdd1.map(lambda x:(x,1))
rdd7.collect()
#%%指定分区规则对数据进行重新分区partitionBy
#pyspark默认是使用HashPartitioner分区
#还有pythonPartitioner，RangePartitioner用于排序使用
#连续使用相同分区器，后面没有意义，分区器定义时出在比较
from typing import Hashable
rdd8 = rdd7.partitionBy(2)
rdd8.saveAsTextFile("/root/Github_files/python_All/Dataset/RDDset")
#%%自定义分区器RDD
#%%groupbykey
list1 = [["java",1],["hadoop",1],["scala",1],["spark",1]]

