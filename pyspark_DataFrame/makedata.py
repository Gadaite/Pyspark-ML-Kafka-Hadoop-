#%%
from re import I
from pyspark import SparkConf,SparkContext
conf = SparkConf().setMaster("local").setAppName("app")
sc = SparkContext(conf=conf)
#%%
#同时从两个文件中读取数据为RDD
rdd = sc.textFile("/root/Github_files/python_All/Dataset/T*.txt")
rdd.collect()
#%%
#将两个文件作为rdd的数据
rdds = sc.wholeTextFiles("/root/Github_files/python_All/Dataset/T*.txt")
rdds.collect()
#%%
#python没有foreach
# 需要Python3，手动定义foreach方法
def foreach(function, iterator):
    for item in iterator:
        function(item)
#%%
foreach(print,rdd.collect())
#%%
#rdd分区
rdd_parttion = sc.textFile("/root/Github_files/python_All/Dataset/T1.txt",2)
rdd_parttion.collect() 
#%%
#保存分区数据并通过文件个数查看分区数(默认为核数)
rdd_parttion.saveAsTextFile("/root/Github_files/python_All/Dataset/RDD_parttion")
#%%
#更新设置默认分区数
conf.set("spark.default.parallelism",4)
#%%
#保存文件，查看默认分区是否修改
rdd.saveAsTextFile("/root/Github_files/python_All/Dataset/RDD_parttion")
#%%
#查看发现无法更改默认分区，set无效但不报错
sc.defaultParallelism
#%%
#分区数据的存储情况
rdd_1 = sc.parallelize([1,2,3,4],3)
rdd_1.collect()
#%%
rdd_1.saveAsTextFile("/root/Github_files/python_All/Dataset/RDD_parttion")
# 1/2/(3,4)
#%%
#数据如何分配到不同分区的：
"""
    @scala_spark:   
    def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
      (0 until numSlices).iterator.map { i =>
        val start = ((i * length) / numSlices).toInt
        val end = (((i + 1) * length) / numSlices).toInt
        (start, end)
      }
    }
"""
#%%
#文件数据分区,读取文件使用的是hadoop的方式
rdd_files = sc.textFile("/root/Github_files/python_All/Dataset/T*.txt",4)
rdd_files.saveAsTextFile("/root/Github_files/python_All/Dataset/RDD_parttion")
#%%
#spark以Hadoop按照行的方式读取数据，不是按照字节读取
#数据读取时以偏移量为单位的，偏移量不会被重新读取
rdd_2 = sc.parallelize([[1,2,3],[4,5,6],[7,8,9]])
rdd_3 = rdd_2.map(lambda x:x[2]**2)
rdd_3.collect()
#%%
#@used to test
list_1 = [1,2,3,4]
list_2 = [i**2 for i in list_1]
list_2
#%%
#转换rdd中的所有元素
rdd_4 = sc.parallelize([[1,2,3],[4,5,6],[7,8,9]])
rdd_5 = rdd_2.map(lambda x:[i**2 for i in x])
rdd_5.collect()
#%%
#%%
rdd_6 = sc.parallelize([1,2,3,4],3)
rdd_6.collect()
#%%
rdd_7 = sc.textFile("/root/Github_files/python_All/Dataset/My_Internship_Experience.txt")
rdd_7.count()#15行
#%%
wordcount_temp1 = rdd_7.flatMap(lambda line:line.split(" "))
wordcount_temp1.collect()
#%%
wordcount_temp2 = wordcount_temp1.map(lambda word:(word,1))
wordcount_temp2.collect()
#%%
wordcount_temp3 = wordcount_temp2.reduceByKey(lambda x,y:x+y)
wordcount_temp3.collect()
#%%
wordcount_temp4 = wordcount_temp3.sortBy(lambda x:x[1],False)
wordcount_temp4.collect()
#%%
wordcount_temp5 = wordcount_temp4.collect()[:10]
wordcount_temp5
#%%
import matplotlib
import matplotlib.pyplot as plt
plt.axes(aspect = 1)
x = [i[1] for i in wordcount_temp5]
y = [i[0] for i in wordcount_temp5]
matplotlib.rcParams["text.color"] = 'w'
plt.pie(x=x,labels=y,autopct="%3.1f %%")
plt.show()

#%%
rdd_8 = sc.parallelize([1,2,3,4,5],3)
rdd_8.collect()
#%%
res1 = sc.runJob(rdd_8,lambda x:[i*i for i in x],[0,1])
res1
#%%
rdd_9 = sc.parallelize([[1,2,3],[4,5,6],[7,8,9]],3)
rdd_9.collect()
#%%
res2 = sc.runJob(rdd_9,lambda x:[[j**2 for j in i] for i in x],[1,2])
res2