#%%
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("QuantileDiscretizer").master("local[*]").getOrCreate()
#%%
values = [(0.1,), (0.4,), (1.2,), (1.5,), (float("nan"),), (float("nan"),)]
df = spark.createDataFrame(values, ["values"])
df.show()
#%%
from pyspark.ml.feature import QuantileDiscretizer
qds = QuantileDiscretizer(numBuckets=2,\
    inputCol="values", outputCol="buckets", relativeError=0.01, handleInvalid="error")
model = qds.fit(df)
model.transform(df).count()
#%%
type(model.transform(df))
#%%
qds.setHandleInvalid("skip").fit(df).transform(df).count()
#%%
splits = model.getSplits()
splits[0]
#%%
spark.stop()