#%%
from pyspark.sql import SparkSession
from pyspark.ml.feature import Binarizer
spark = SparkSession.builder.config("spark.Driver.host","192.168.1.10")\
    .config("spark.ui.showConsoleProgress","false")\
    .appName("Binarize").master("local[*]").getOrCreate()
#%%
data = spark.createDataFrame([
    (0.1,),
    (2.3,),
    (1.1,),
    (4.2,),
    (2.5,),
    (6.8,),
],["values"])
data.show()
#%%
binarizer = Binarizer(threshold=2.4,inputCol="values",outputCol="features")
#%%
res = binarizer.transform(data)
res.show()
#%%
res.printSchema()
#%%
res.head().features
#%%
# spark.stop()