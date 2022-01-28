#%%
from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.Driver.host","192.168.1.4")\
    .config("spark.ui.showConsoleProgress","false")\
    .appName("Normalizer").master("local[*]").getOrCreate()
#%%
from pyspark.ml.linalg import Vectors
svec = Vectors.sparse(4, {1: 4.0, 3: 3.0})
df = spark.createDataFrame([(Vectors.dense([3.0, -4.0]), svec)], ["dense", "sparse"])
df.show()
df.printSchema()
#%%
from pyspark.ml.feature import Normalizer
normalizer = Normalizer(p=2.0, inputCol="dense", outputCol="features")
normalizer.transform(df).show()
normalizer.transform(df).head(1)
#%%
normalizer.setParams(inputCol="sparse", outputCol="freqs").transform(df).show()
#%%
params = {normalizer.p: 1.0, normalizer.inputCol: "dense", normalizer.outputCol: "vector"}
normalizer.transform(df, params).show()
normalizer.transform(df, params).head(1)
#%%
spark.stop()
#%%