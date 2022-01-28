#%%
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("SQLTransformer").master("local[*]").getOrCreate()
from pyspark.ml.linalg import Vectors
df = spark.createDataFrame([(Vectors.dense([0.0]),), (Vectors.dense([2.0]),)], ["a"])
df.show()
df.printSchema()
#%%
from pyspark.ml.feature import StandardScaler
standardScaler = StandardScaler(inputCol="a", outputCol="scaled")
model = standardScaler.fit(df)
model.transform(df).show()
model.mean
#%%
spark.stop()