#%%
from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.Driver.host","192.168.1.4")\
    .config("spark.ui.showConsoleProgress","false")\
    .appName("ChiSqselector").master("local[*]").getOrCreate()
#%%
from pyspark.ml.linalg import Vectors
df = spark.createDataFrame(
   [(Vectors.dense([0.0, 0.0, 18.0, 1.0]), 1.0),
    (Vectors.dense([0.0, 1.0, 12.0, 0.0]), 0.0),
    (Vectors.dense([1.0, 0.0, 15.0, 0.1]), 0.0)],
["features", "label"])
df.show()
#%%
from pyspark.ml.feature import ChiSqSelector
selector = ChiSqSelector(numTopFeatures=1, outputCol="selectedFeatures")
model = selector.fit(df)
model.transform(df).show()
#%%
selector2 = ChiSqSelector(numTopFeatures=2, outputCol="selectedFeatures")
model2 = selector2.fit(df)
model2.transform(df).show()
#%%
spark.stop()