#%%
from pyspark.ml.feature import CountVectorizer
from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.driver.host","192.168.1.4")\
    .config("spark.ui.showConsoleProgress","false")\
    .appName("CountVectorizer").master("local[*]").getOrCreate()
#%%
data = spark.createDataFrame([
    (0, ["a", "b", "c"]),
    (1, ["a", "b", "b", "c", "a"]),
    (2,["d","d","d","b","c","b","a"]),
    (3,["a","d","f"]),
    (4,["a","e","a","a"]),
    (5,["e","f","f","d"]),
],["label","raw"])
data.show()
#%%
data.printSchema()
#%%
cv = CountVectorizer(inputCol="raw", outputCol="vectors")
model = cv.fit(data)
model.transform(data).show()
#%%
resdata = model.transform(data)
resdata.head(6)
#%%
resdata.printSchema()
