#%%
from pyspark.sql import SparkSession
from pyspark.ml.feature import StopWordsRemover
spark = SparkSession.builder.config("spark.driver.host","192.168.1.4")\
    .config("spark.ui.showConsoleProgress","false")\
    .appName("StopWordsRemover").master("local[*]").getOrCreate()
#%%
data = spark.createDataFrame([
    (["Beautiful","is","better","than","ugly"],),
    (["Explicit","is","better","than","implicit"],),
    (["Simple","is","better","than","complex"],),
    (["Complex","is","better","than","complicated"],),
    (["Flat","is","better","than","nested"],),
    (["Sparse","is","better","than","dense"],)
],["text"])
data.show()
#%%
data.head(6)
#%%
stopWordsRemover = StopWordsRemover(inputCol="text",outputCol="res",stopWords=["is","better","than"])
data = stopWordsRemover.transform(data)
data.show()
#%%
data.head(6)
#%%
data.printSchema()
#%%
spark.stop()