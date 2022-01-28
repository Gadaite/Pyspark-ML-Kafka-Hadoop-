#%%
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("VectorSizeHint").master("local[*]").getOrCreate()
sent = ("a b " * 100 + "a c " * 10).split(" ")
doc = spark.createDataFrame([(sent,), (sent,)], ["sentence"])
doc.head(2)
#%%
doc.printSchema()
#%%
from pyspark.ml.feature import Word2Vec
word2Vec = Word2Vec(vectorSize=5, seed=42, inputCol="sentence", outputCol="model")
model = word2Vec.fit(doc)
model.getVectors().show()
model.getVectors().head(3)
#%%
model.findSynonymsArray("a", 2)
#%%
from pyspark.sql.functions import format_number as fmt
model.findSynonyms("a", 2).select("word", fmt("similarity", 5).alias("similarity")).show()
#%%
model.transform(doc).head()
#%%
spark.stop()