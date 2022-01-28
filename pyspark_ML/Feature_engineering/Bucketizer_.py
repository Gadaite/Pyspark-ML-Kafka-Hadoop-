#%%
from pyspark.sql import SparkSession
from pyspark.ml.feature import Bucketizer
spark = SparkSession.builder.config("spark.Driver.host","192.168.1.3")\
    .config("spark.ui.showConsoleProgress","false")\
    .appName("Bucketizer").master("local[*]").getOrCreate()
#%%
values = [(0.1,), (0.4,), (1.2,), (1.5,), (float("nan"),), (float("nan"),)]
df = spark.createDataFrame(values, ["values"])
bucketizer = Bucketizer(splits=[-float("inf"), 0.5, 1.4, float("inf")],
    inputCol="values", outputCol="buckets")
bucketed = bucketizer.setHandleInvalid("keep").transform(df)
bucketed.show()
#%%
bucketizer.setParams(outputCol="res").transform(df).show()
#%%
spark.stop()






#%%
spark.stop()