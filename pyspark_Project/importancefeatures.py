#%%
from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.driver.host","192.168.1.5")\
    .config("spark.ui.showConsoleProgress","false")\
    .appName("importancefeatures").master("local[*]").getOrCreate()

#%%
data = spark.read.csv("/mnt/f/Code/DataSet/MLdataset/dog_food.csv",header=True,inferSchema=True)
data.printSchema()
#%%
data.show(3)
#%%
from pyspark.ml.feature import VectorAssembler
vectorAssembler = VectorAssembler(inputCols=["A","B","C","D"],outputCol="features")
datavector = vectorAssembler.transform(data)
datavector.show()
#%%
datavector = datavector.select("features","Spoiled")
datavector.show(3)
#%%
from pyspark.ml.classification import RandomForestClassifier
rf = RandomForestClassifier(featuresCol="features",labelCol="Spoiled")
#%%
model = rf.fit(datavector)
# %%
datavector.show()
#%%
model.featureImportances
# %%
spark.stop()