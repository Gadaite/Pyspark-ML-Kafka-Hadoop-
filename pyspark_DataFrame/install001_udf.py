#%%
from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession, udf
from pyspark.sql.types import Row
spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
sc = spark.sparkContext
#%%
list = [[1,2,3],[4,5,6],[7,8,9]]
rdd1 = sc.parallelize(list)
rdd1.collect()
#%%
df = spark.createDataFrame(rdd1,schema=["A","B","C"])
df.show()
#%%
from pyspark.sql.types import *
import pyspark.sql.functions as F
#%%
def func1(array):
    if(array%2==0):
        return 1
    else:
        return 0
udf_func1 = F.udf(func1, IntegerType())
#%%
dfs = df.withColumn("D",udf_func1("A"))
#%%
dfs.createOrReplaceTempView("table")
#%%
spark.sql("""
    select * from table
    """).show()
#%%
df.show()
#%%
spark.stop()
#%%