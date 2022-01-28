#%%
from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.driver.host","192.168.1.4")\
    .config("spark.ui.showConsoleProgress","false")\
    .appName("hark").master("local[*]").getOrCreate()
# %%
data = spark.read.csv("/mnt/e/win_ubuntu/Code/DataSet/MLdataset/hack_data.csv",header=True,inferSchema=True)
data.show(3)
data.printSchema()
#%%
cols = data.columns
cols.remove("Location")
cols
#%%
from pyspark.ml.feature import VectorAssembler
vectorAssembler = VectorAssembler(inputCols=cols,outputCol="features")
data_temp = vectorAssembler.transform(data)
data_temp.head(3)

#%%
from pyspark.ml.feature import StandardScaler
standardScaler = StandardScaler(inputCol="features",outputCol="scalerfeatures")
model = standardScaler.fit(data_temp)
#%%
data_temp = model.transform(data_temp).select("features","scalerfeatures")
data_temp.head(3)
#%%
from pyspark.ml.clustering import KMeans
kmeans_2 = KMeans(featuresCol="scalerfeatures",k=2)
kmeans_4 = KMeans(featuresCol="scalerfeatures",k=4)
#%%
model_2 = kmeans_2.fit(data_temp)
model_2.computeCost(data_temp)
model_2.transform(data_temp).groupBy("prediction").count().show()
#%%
model_4 = kmeans_4.fit(data_temp)
model_4.computeCost(data_temp)
model_4.transform(data_temp).groupBy("prediction").count().show()