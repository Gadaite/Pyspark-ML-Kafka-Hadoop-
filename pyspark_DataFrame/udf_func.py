#%%
from pyspark import SparkConf,SparkContext, rdd
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("app")\
    .getOrCreate()
sc = spark.sparkContext
#%%
import pyspark.sql.functions as F
from pyspark.sql.types import *
#%%
list = [[1,2],[3,4],[5,6]]
rdd = sc.parallelize(list)
rdd.collect()
#%%
rdd.glom()
#%%
df = spark.createDataFrame(rdd,schema=["A","B"])
df.show()
#%%
def function(p1,p2):
    return p1+p2
udf_function = F.udf(function,IntegerType())
udf_function
#%%
dfs = df.withColumn("A+B",udf_function("A","B"))
dfs.show()
#%%
spark.stop()
#%%