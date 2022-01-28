#%%
from json import load
from pyspark.sql import SparkSession
from pyspark.sql.context import SQLContext
spark = SparkSession.builder.master("local[*]").appName("app").getOrCreate()
sc = spark.sparkContext
#%%
indf = spark.read.format("jdbc")\
    .option("driver","com.mysql.jdbc.Driver")\
    .option("url","jdbc:mysql://139.155.70.177:3306/Gadaite")\
    .option("user","root")\
    .option("password","zzjz123")\
    .option("dbtable","titanic")\
    .load()
#%%
indf.printSchema()
#%%
indf.show()
#%%
indf.count()
#%%
indf.columns
