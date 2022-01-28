#%%
from pyspark.sql import SparkSession
from pyspark.sql.functions import corr
from pyspark.mllib.stat import Statistics
spark = SparkSession.builder.config("spark.driver.host","192.168.1.4")\
    .config("spark.ui.showConsoleProgress","false")\
    .appName("correlation_coefficient").master("local[*]").getOrCreate()
#%%
sc = spark.sparkContext
#%%
rdd1 = sc.parallelize([1,2,3,4,5,6,7])
rdd2 = sc.parallelize([2,4,6,8,10,12,13])
M1_res = Statistics.corr(rdd1,rdd2)
M1_res
#%%
from pyspark.sql.types import Row
lst = []
for i in range(0,len(rdd1.collect())):
    a = rdd1.collect()[i]
    b = rdd2.collect()[i]
    lst.append([a,b])
lst
#%%
df = spark.createDataFrame(lst,["col1","col2"])
df.show()
#%%
type(df.select(corr("col1","col2")).collect()[0][0])
M2_res = df.select(corr("col1","col2")).collect()[0][0]
#%%
print(M1_res,M2_res)
#%%
