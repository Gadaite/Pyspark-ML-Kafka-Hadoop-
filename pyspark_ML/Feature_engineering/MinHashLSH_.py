#%%
from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.Driver.host","192.168.1.4")\
    .config("spark.ui.showConsoleProgress","false")\
    .appName("MinHashLSH").master("local[*]").getOrCreate()
#%%
from pyspark.ml.linalg import Vectors
from pyspark.sql.functions import col
data = [(0, Vectors.sparse(6, [0, 1, 2], [1.0, 1.0, 1.0]),),
        (1, Vectors.sparse(6, [2, 3, 4], [1.0, 1.0, 1.0]),),
        (2, Vectors.sparse(6, [0, 2, 4], [1.0, 1.0, 1.0]),)]
df = spark.createDataFrame(data, ["id", "features"])
df.show()
#%%
df.head(3)
#%%
from pyspark.ml.feature import MinHashLSH
mh = MinHashLSH(inputCol="features", outputCol="hashes", seed=12345)
model = mh.fit(df)
model.transform(df).show()
#%%
model.transform(df).head(3)
#%%
data2 = [(3, Vectors.sparse(6, [1, 3, 5], [1.0, 1.0, 1.0]),),
         (4, Vectors.sparse(6, [2, 3, 5], [1.0, 1.0, 1.0]),),
         (5, Vectors.sparse(6, [1, 2, 4], [1.0, 1.0, 1.0]),)]
df2 = spark.createDataFrame(data2, ["id", "features"])
df2.show()
#%%
key = Vectors.sparse(6, [1, 2], [1.0, 1.0])
model.approxNearestNeighbors(df2, key, 1).show()
#%%
model.approxSimilarityJoin(df, df2, 0.6, distCol="JaccardDistance").select(
    col("datasetA.id").alias("idA"),
    col("datasetB.id").alias("idB"),
    col("JaccardDistance")).show()
#%%
spark.stop()