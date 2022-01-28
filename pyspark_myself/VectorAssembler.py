#%%
from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
sc = spark.sparkContext
#创建spark对象,sc对象

#%%
list_test = [(1,2,3),(12,45,76),(35,76,21)]
rdd_test = sc.parallelize(list_test)
print(rdd_test.collect())
#生成rdd，便于之后转换为df

#%%
df_test = spark.createDataFrame(rdd_test,schema=["col1","col2","col3"])
df_test.show()
df_test.printSchema()
#创建DataFrame，查看数据表，和表结构

#%%
from pyspark.ml.feature import VectorAssembler
#导入VectorAssembler

#%%
#"""vecassembler = VectorAssembler(inputCols=["col1","col2","col3"],outputCol="features")"""
#等价于以下这行代码
vecassembler = VectorAssembler(inputCols=df_test.columns,outputCol="features")
df_out = vecassembler.transform(df_test)
df_out.show()
df_out.printSchema()
#生成的新的vector，作为新的一列数据

#%%
"""
    总结：
    VectorAssembler将多个数值列按顺序汇总成一个向量列。
"""

