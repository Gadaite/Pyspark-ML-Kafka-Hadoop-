#%%
# from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.driver.host","192.168.1.10").getOrCreate()
sc = spark.sparkContext
# %%
list =[(1,2),(3,4)]
rdd = sc.parallelize(list,1)
print(type(rdd))
#%%
rdd.collect()
type(rdd.collect())
#%%
df = spark.createDataFrame(rdd)
df.show()
#%%
df.createOrReplaceTempView("mytable")
#%%
spark.sql("""
    select * from mytable
    """).show()
list_other = [(-1,-2),(-3,-4)]
rdd_other = sc.parallelize(list_other)
rdd_other.collect()
#%%
df_other = spark.createDataFrame(rdd_other)
df_other.show()
#%%
df_other.createOrReplaceTempView("mytable_other")
spark.sql("""
    select * from mytable_other
    """).show()
#%%
#dataframe纵向合并
dfs = df.union(df_other)
dfs.show()
# %%
#dataframe横向合并


# #%%
# print("hello")
#%%

