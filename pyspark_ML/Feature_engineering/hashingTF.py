#%%
from pyspark.sql import SparkSession
from pyspark.ml.feature import HashingTF
#%%
spark = SparkSession.builder.config("spark.driver.host","192.168.1.4")\
    .config("spark.ui.showConsoleProgress","false")\
    .appName("HashingTF").master("local[*]").getOrCreate()
#%%
data = spark.createDataFrame([
    (["I","am","zhangsan","zhangsan","learn","spark"],),
    (["spark","is","perfect"],),
    (["I","want","to","learn","spark"],)
],["text"])
data.show()
data.printSchema()
#%%
hashingTF = HashingTF(inputCol="text",outputCol="hashingTF_Res",numFeatures=200)
resHashTF = hashingTF.transform(data)
resHashTF.show()
#%%
from pyspark.ml.feature import CountVectorizer
countVectorizer = CountVectorizer(inputCol="text",outputCol="countVectorizer_RES")
model = countVectorizer.fit(data)
resHountVectorizer = model.transform(data)
resHountVectorizer.show()
#%%
resHashTF.head(3)
#%%