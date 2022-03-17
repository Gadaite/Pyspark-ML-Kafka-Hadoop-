#%%
from pyspark.sql import SparkSession
from pyspark.ml.feature import IDF, HashingTF, Tokenizer
spark = SparkSession.builder.config("spark.driver.host","192.168.1.10")\
    .config("spark.ui.showConsoleProgress","false")\
    .appName("IDF").master("local[*]").getOrCreate()
#%%
data = spark.createDataFrame([
    (0, "Hi I heard about Spark"),
    (0, "I wish Java could use case classes"),
    (1, "Logistic regression models are neat")
], ["label", "sentence"])
data.show()
#%%
tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
data = tokenizer.transform(data)
hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=20)
featurizedData = hashingTF.transform(data)
featurizedData.show()
#%%
featurizedData.printSchema()
#%%
featurizedData.head(1)
#%%
idf = IDF(inputCol="rawFeatures",outputCol="IDF",minDocFreq=4)
model = idf.fit(featurizedData)
IDFData = model.transform(featurizedData)
IDFData.show()
#%%
IDFData.select("rawFeatures","IDF").head(3)
#%%
spark.stop()