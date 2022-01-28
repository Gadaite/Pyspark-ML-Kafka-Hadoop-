#%%
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("app").master("local[*]").getOrCreate()
jdbcdf = spark.read.format("jdbc")\
            .option("driver","org.postgresql.Driver")\
            .option("url","jdbc:postgresql://192.168.1.10:5432/trajectory")\
            .option("dbtable","lastappeared")\
            .option("user","postgres")\
            .option("password","LYP809834049")\
            .load()
jdbcdf.show()

