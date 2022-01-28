#%%
from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.driver.host","192.168.1.5")\
    .config("spark.ui.showConsoleProgress","false").appName("test").master("local[*]").getOrCreate()
#%%
spark.stop()