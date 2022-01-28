#%%
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("RegexTokenizer").master("local[*]").getOrCreate()
#%%
df = spark.createDataFrame([("Optional parameters also allow filtering tokens",)], ["text"])
df.head(1)
#%%
from pyspark.ml.feature import RegexTokenizer
reTokenizer = RegexTokenizer(inputCol="text", outputCol="words",minTokenLength=1)
reTokenizer.transform(df).head()
#%%
reTokenizer.setParams(minTokenLength=6).transform(df).head(1)
#%%
reTokenizer.setParams(gaps=False,minTokenLength=6).transform(df).head(1)
#%%
spark.stop()