#%%
from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
sc = spark.sparkContext
#%%
list_A = [(1,2),(2,4)]
list_B = [(3,6),(4,8)]
rdd_A = sc.parallelize(list_A)
rdd_B = sc.parallelize(list_B)
#%%
rdd_A.collect()
#%%
rdd_B.collect()
#%%
df_A = spark.createDataFrame(rdd_A,schema=["A_col1","A_col2"])
df_B = spark.createDataFrame(rdd_B,schema=["B_col1","b_col2"])
df_A.show()
df_B.show()
#%%
#中间表定义
df_temp = spark.createDataFrame(sc.parallelize([(1,11),(3,33)]),\
    schema=["A_col1","A_other_info"])
df_temp.show()
df_temp.printSchema()
#%%
df_A.createOrReplaceTempView("df_A")
df_B.createOrReplaceTempView("df_B")
df_temp.createOrReplaceTempView("df_temp")
#%%
#查询模拟情况
spark.sql("""
    select df_temp.A_col1,A_col2,A_other_info 
    from df_A,df_temp 
    where df_A.A_col1 == df_temp.A_col1
    """).show()
#%%


#%%
#%%
spark.sql("""
    select * from df_A
    """).show()
#%%
spark.stop()