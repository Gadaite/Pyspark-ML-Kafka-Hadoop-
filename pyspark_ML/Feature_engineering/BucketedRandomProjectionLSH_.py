#%%
from pyspark.sql import SparkSession
from pyspark.ml.feature import BucketedRandomProjectionLSH
spark = SparkSession.builder.config("spark.Driver.host","192.168.1.4")\
    .config("spark.ui.showConsoleProgress","false")\
    .appName("BucketedRandomProjectionLSH").master("local[*]").getOrCreate()
#%%
from pyspark.ml.linalg import Vectors
from pyspark.sql.functions import col
data = [(0, Vectors.dense([-1.0, -1.0 ]),),
        (1, Vectors.dense([-1.0, 1.0 ]),),
        (2, Vectors.dense([1.0, -1.0 ]),),
        (3, Vectors.dense([1.0, 1.0]),)]
df = spark.createDataFrame(data, ["id", "features"])
df.show()
#%%
brp = BucketedRandomProjectionLSH(inputCol="features", outputCol="hashes",seed=12345, bucketLength=1.0)
#%%
model = brp.fit(df)
model.transform(df).show()
#%%
data2 = [(4, Vectors.dense([2.0, 2.0 ]),),
         (5, Vectors.dense([2.0, 3.0 ]),),
         (6, Vectors.dense([3.0, 2.0 ]),),
         (7, Vectors.dense([3.0, 3.0]),)]
df2 = spark.createDataFrame(data2, ["id", "features"])
df2.show()
#%%
model.approxNearestNeighbors(df2, Vectors.dense([1.0, 2.0]), 1).show()
#%%
model.approxNearestNeighbors(df2, Vectors.dense([1.0, 2.0]), 2).show()
#%%
model.approxSimilarityJoin(df, df2, 3.0, distCol="EuclideanDistance")\
    .select(col("datasetA.id").alias("idA"),
        col("datasetB.id").alias("idB"),
        col("EuclideanDistance")).show()
#%%
model.approxSimilarityJoin(df, df2, 3.0, distCol="EuclideanDistance").select(
    col("datasetA.id").alias("idA"),
    col("datasetB.id").alias("idB"),
    col("EuclideanDistance")).show()
#%%
spark.stop()