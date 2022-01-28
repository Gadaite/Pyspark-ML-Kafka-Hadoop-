#%%
from pyspark.sql import SparkSession
from pyspark.sql.functions import log
spark = SparkSession.builder.appName("APP").getOrCreate()
sc = spark.sparkContext
#%%
from pyspark.sql.types import *
#%%
schema = StructType([
    StructField("column_A",IntegerType()),
    StructField("column_B",IntegerType()),
    StructField("column_C",IntegerType()),
])
#%%
list_ori = [[1,2,3],[4,5,6],[7,8,9]]
rdd_ori = sc.parallelize(list_ori)
type(rdd_ori.collect()[0])
#%%
df_ori = spark.createDataFrame(rdd_ori,schema=schema)
#%%
df_ori.show()
#%%
"""
    collect():
    1.collect()转换为List       List[Row(),Row().....]
    2.collect()[i]提取到Row()   Row(z,b,c,d,e)
    3.collect()[i][j]提取到具体的值     eg：c
"""
#%%
from pyspark.sql import Row
#df转rdd
ori_rdd = df_ori.rdd
temp_rdd = ori_rdd.map(lambda x:x.asDict())
temp_rdd['column_D'] = temp_rdd['column_A'][::-1]
#%%
ori_rdd.collect()[0]
#%%
ori_rdd.collect()[0][2]
#%%
df_ori.createOrReplaceTempView("view")
#%%
#通过sparksql对dataframe新增一列
outdf = spark.sql("""
    select *,(column_A+column_B+column_C) as column_Sum 
    from view
    """).show()
#%%
#使用spark自带函数
from pyspark.sql.functions import *
#%%
df_log = df_ori.withColumn("colum_multiply",df_ori['column_A']*df_ori['column_B']*df_ori['column_C'])
df_log.show()
#%%
#使用rdds创建dataframe
df_rdd = spark.createDataFrame(temp_rdd)
df_rdd.show()