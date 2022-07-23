#%%
from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
# from pyspark.sql.group import df_varargs_api
spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
sc = spark.sparkContext
#%%
jdbcDF = spark.read.format("jdbc")\
    .option("driver","com.mysql.cj.jdbc.Driver")\
    .option("url","jdbc:mysql://192.168.1.10:3306/Gadaite")\
    .option("dbtable","audi")\
    .option("user","root")\
    .option("password","LYP809834049")\
    .load()
#%%
jdbcDF.createOrReplaceTempView("table")
#%%
inputdf = spark.sql("select * from table limit 10")
inputdf.show()
#%%
len(jdbcDF.columns)
#%%
for row in inputdf.collect():
    ret =[row[i] for i in range(0,len(jdbcDF.columns))]
    break
print(ret)
#%%
for row in inputdf.collect():
    [row[i] for i in range(0,len(jdbcDF.columns))]
    break
print(ret)
#%%
inputdf.collect()[0][0]
#%%
#%%
#dataframe转rdd
rdd1 = inputdf.rdd
type(rdd1)
#%%
rdd1.glom()
#%%
rdd1.collect()
#%%
rdd1.repartition(5)
#%%
rdd1.saveAsTextFile("/root/Github_files/python_All/Dataset/RDDset")
#%%
inputdf.show()
#%%
def func1(str):
    return str.lstrip().rstrip()
#%%
import pyspark.sql.functions as F
from pyspark.sql.types import *
#%%
udf_func1 = F.udf(func1,StringType())
#%%
inputdf.collect()
#%%

def foreach(fn,iterable):
    for x in iterable:
        fn(x)
#%%
foreach(print,inputdf.collect())
#%%
spark.stop()