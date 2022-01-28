#%%
from pyspark.sql import SparkSession
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import VectorAssembler
spark = SparkSession.builder.config("spark.driver.host","192.168.1.10")\
    .config("spark.ui.showConsoleProgress","false")\
    .appName("RandomForestClassifier").master("local[*]").getOrCreate()
sc = spark.sparkContext
#%%
data = spark.createDataFrame([
    (1,1,-1221),
    (2,1,-464),
    (-3,0,-1),
    (-4,0,2),
    (100,1,7),
    (5,1,2)
],["col1","res","col2"])
data.show()
#%%
data = data.select("col1","col2","res")
data.show()
# %%
vectorAssembler = VectorAssembler(inputCols=["col1","col2"],outputCol="features")
datavector = vectorAssembler.transform(data)
datavector.show()
#%%
datachange = datavector.select("features","res")
datachange.show()
#%%
randomForestClassifier = RandomForestClassifier(featuresCol="features",labelCol="res")
model = randomForestClassifier.fit(datachange)
#%%
model.featureImportances
#%%
datatest = spark.createDataFrame([
    (5,1),
    (2,-1),
    (-9,0),
    (-4,0),
    (-100,-1),
    (5,1),
    (-200,-3)
],["col1","col2"])
datatest.show()
#%%
testvector = vectorAssembler.transform(datatest)
testvector.show()
#%%
testres = model.transform(testvector)
testres.show()
#%%
spark.stop()