#%%
from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
sc = spark.sparkContext
#%%
jdbcDF = spark.read.format("jdbc")\
    .option("user","root")\
    .option("driver","com.mysql.cj.jdbc.Driver")\
    .option("password","LYP809834049")\
    .option("dbtable","seeds_dataset")\
    .option("url","jdbc:mysql://192.168.1.10:3306/Gadaite")\
    .load()
#%%
jdbcDF.show()
#%%
jdbcDF.createOrReplaceTempView("seeds")
#%%
jdbcDF.printSchema()
#%%
