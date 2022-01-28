#%%
from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.Driver.host","192.168.1.4")\
    .config("spark.ui.showConsoleProgress","false")\
    .appName("NGram").master("local[*]").getOrCreate()
#%%
from pyspark.sql.types import Row
df = spark.createDataFrame([Row(inputTokens=["a", "b", "c", "d", "e"])])
df.show()
df.printSchema()
#%%
from pyspark.ml.feature import NGram
ngram = NGram(inputCol="inputTokens",outputCol="res")
ngram.transform(df).show()
#%%
ngram.setParams(n=3)
ngram.transform(df).show()
ngram.transform(df).head(1)
#%%
spark.stop()