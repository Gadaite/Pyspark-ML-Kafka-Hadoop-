#%%
from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.Driver.host","192.168.1.4")\
    .config("spark.ui.showConsoleProgress","false")\
    .appName("MaxAbsScaler").master("local[*]").getOrCreate()
#%%
from pyspark.ml.linalg import Vectors
df = spark.createDataFrame([(Vectors.dense([1.0]),), (Vectors.dense([2.0]),), (Vectors.dense([10.0]),)], ["a"])
df.show()
#%%
from pyspark.ml.feature import MaxAbsScaler
maxAbsScaler = MaxAbsScaler(inputCol="a",outputCol="res")
model = maxAbsScaler.fit(df)
model.transform(df).show()
#%%
spark.stop()