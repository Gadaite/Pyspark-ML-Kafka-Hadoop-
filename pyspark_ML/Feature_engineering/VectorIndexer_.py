#%%
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("SQLTransformer").master("local[*]").getOrCreate()
from pyspark.ml.linalg import Vectors
df = spark.createDataFrame([(Vectors.dense([-1.0, 0.0]),),
    (Vectors.dense([0.0, 1.0]),), (Vectors.dense([0.0, 2.0]),)], ["a"])
df.show()
#%%
from pyspark.ml.feature import VectorIndexer
indexer = VectorIndexer(maxCategories=2, inputCol="a", outputCol="indexed")
model = indexer.fit(df)
model.transform(df).show()
#%%
model.numFeatures
#%%
model.categoryMaps
#%%
params = {indexer.maxCategories: 3, indexer.outputCol: "vector"}
model2 = indexer.fit(df, params)
model2.transform(df).show()
#%%
spark.stop()