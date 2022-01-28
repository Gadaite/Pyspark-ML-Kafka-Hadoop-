#%%
from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.Driver.host","192.168.1.5")\
    .config("spark.ui.showConsoleProgress","false")\
    .appName("PolynomialExpansion").master("local[*]").getOrCreate()
#%%
from pyspark.ml.linalg import Vectors
df = spark.createDataFrame([(Vectors.dense([0.5, 2.0]),)], ["dense"])
df.show()
#%%
from pyspark.ml.feature import PolynomialExpansion
px = PolynomialExpansion(degree=2,inputCol="dense",outputCol="res")
px.transform(df).show()
px.transform(df).head(1)
#%%
px.setParams(degree=3)
px.transform(df).head(1)
#%%
spark.stop()