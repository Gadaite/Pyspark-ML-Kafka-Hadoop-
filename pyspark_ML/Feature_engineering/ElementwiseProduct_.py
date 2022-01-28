#%%
from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.Driver.host","192.168.1.3")\
    .config("spark.ui.showConsoleProgress","false")\
    .appName("ElementwiseProduct").master("local[*]").getOrCreate()
#%%
from pyspark.ml.linalg import Vectors
df = spark.createDataFrame([(Vectors.dense([2.0, 1.0, 3.0]),)], ["values"])
df.show()
#%%
from pyspark.ml.feature import ElementwiseProduct
elementwiseProduct = ElementwiseProduct(inputCol="values",scalingVec=Vectors.dense([9.0,8.0,7.0]),outputCol="res")
elementwiseProduct.transform(df).show()
#%%
elementwiseProduct2 = ElementwiseProduct(inputCol="values",scalingVec=Vectors.dense([9.0,2.0,3.0]),outputCol="res")
elementwiseProduct2.transform(df).show()
#%%
elementwiseProduct2.setParams(scalingVec=Vectors.dense([1.0,2.0,3.0]))
elementwiseProduct2.transform(df).show()
#%%
spark.stop()