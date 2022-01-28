#%%
from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.Driver.host","192.168.1.5")\
    .config("spark.ui.showConsoleProgress","false")\
    .appName("PCA").master("local[*]").getOrCreate()
#%%
from pyspark.ml.linalg import Vectors
data = [(Vectors.sparse(5, [(1, 1.0), (3, 7.0)]),),
    (Vectors.dense([2.0, 0.0, 3.0, 4.0, 5.0]),),
    (Vectors.dense([4.0, 0.0, 0.0, 6.0, 7.0]),)]
df = spark.createDataFrame(data,["features"])
df.show()
#%%
df.head(3)
#%%
df.printSchema()
#%%
from pyspark.ml.feature import PCA
pca = PCA(k=2,inputCol="features",outputCol="res")
model = pca.fit(df)
model.transform(df).show()
#%%
model.transform(df).head(3)
#%%
model.explainedVariance
#%%
spark.stop()