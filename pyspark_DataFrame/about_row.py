#%%
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("app").master("local").getOrCreate()
#%%
jdbcDF = spark.read.format("jdbc")\
    .option("driver","com.mysql.jdbc.Driver")\
    .option("url","jdbc:mysql://139.155.70.177:3306/Gadaite")\
    .option("dbtable","audi")\
    .option("user","root")\
    .option("password","zzjz123")\
    .load()
#%%
count = 0
for row in jdbcDF.collect():
    count = count +1
    ret =[row[i] for i in range(0,len(jdbcDF.columns))]
    if(count == 2+1):
        break
    print(ret)
    print(type(row))
    print(row)
#%%
spark.stop()