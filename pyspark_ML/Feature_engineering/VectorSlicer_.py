#%%
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("VectorSizeHint").master("local[*]").getOrCreate()
from pyspark.ml.linalg import Vectors
df = spark.createDataFrame([
    (Vectors.dense([-2.0, 2.3, 0.0, 0.0, 1.0]),),
    (Vectors.dense([0.0, 0.0, 0.0, 0.0, 0.0]),),
    (Vectors.dense([0.6, -1.1, -3.0, 4.5, 3.3]),)], ["features"])
#%%
df.head(3)
#%%
from pyspark.ml.feature import VectorSlicer
vs = VectorSlicer(inputCol="features", outputCol="sliced", indices=[1, 4])
vs.transform(df).head(3)
#%%
spark.stop()