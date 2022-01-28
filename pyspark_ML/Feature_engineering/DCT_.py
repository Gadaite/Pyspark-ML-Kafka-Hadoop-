#%%
from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.Driver.host","192.168.1.3")\
    .config("spark.ui.showConsoleProgress","false")\
    .appName("DCT").master("local[*]").getOrCreate()
#%%
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import DCT
df1 = spark.createDataFrame([(Vectors.dense([5.0, 8.0, 6.0]),)], ["vec"])
df1.show()
#%%
dct = DCT(inverse=False, inputCol="vec", outputCol="resultVec")
df2 = dct.transform(df1)
df2.show()
#%%
df2.head(1)
#%%
df3 = DCT(inverse=True, inputCol="resultVec", outputCol="origVec").transform(df2)
df3.show()
#%%
df3.head(1)
#%%
spark.stop()
