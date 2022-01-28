#%%
from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.Driver.host","192.168.1.4")\
    .config("spark.ui.showConsoleProgress","false")\
    .appName("MinMaxScaler").master("local[*]").getOrCreate()
#%%
from pyspark.ml.linalg import Vectors
df = spark.createDataFrame([(Vectors.dense([0.0]),), 
    (Vectors.dense([2.0]),),
    (Vectors.dense([12.0]),)], ["a"])
df.show()
#%%
from pyspark.ml.feature import MinMaxScaler
mmScaler = MinMaxScaler(inputCol="a", outputCol="scaled")
model = mmScaler.fit(df)
model.transform(df).show()
#%%
model.originalMin
#%%
model.originalMax
#%%
spark.stop()