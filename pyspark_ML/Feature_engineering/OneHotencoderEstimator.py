#%%
from pyspark.sql import SparkSession
from pyspark.ml.feature import OneHotEncoderEstimator
spark = SparkSession.builder.config("spark.driver.host","192.168.1.4")\
    .config("spark.ui.showConsoleProgress","false")\
    .master("local[*]").appName("OneHotEncoderEstimator").getOrCreate()
sc = spark.sparkContext
#%%
data = spark.createDataFrame([
    ("hadoop",3.0),
    ("spark",4.0),
    ("flink",0.0),
    ("kafka",1.0),
    ("java",2.0),
    ("flink",0.0),
    ("kafka",1.0),
    ("python",5.0)
], ["key", "value"])
data.show()
#%%
oneHotEncoderEstimator = OneHotEncoderEstimator(inputCols=["value"],outputCols=["res"])
#%%
model = oneHotEncoderEstimator.fit(data)
res = model.transform(data)
res.show()
#%%
data.printSchema()
#%%
res.printSchema()