#%%
#生成一个SparkSession对象spark
from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
sc =spark.sparkContext
#%%
#读取数据集并转换为spark的dataframe，并打印其结构
df = spark.read.csv("/root/python_All/Dataset/seeds_dataset.csv", inferSchema=True, header=True)
df.show(4)
df.printSchema()
#%%

#%%
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
#%%
assembler = VectorAssembler(inputCols = df.columns, outputCol = 'features')
final_df = assembler.transform(df)
final_df.show(4)
#%%
final_df.printSchema()
#%%
from pyspark.ml.feature import StandardScaler
scaler = StandardScaler(inputCol = 'features', outputCol = 'scaledFeatures')
scaler_model = scaler.fit(final_df)
final_df = scaler_model.transform(final_df)
final_df.show(4)

#%%
import pandas as pd
df_pd = final_df.toPandas()
df_pd
#%%
final_df.take(1)
#%%
kmeans = KMeans(featuresCol = 'scaledFeatures', k=3)
model = kmeans.fit(final_df)
#%%
print('WSSSE:', model.computeCost(final_df))
#%%
centers = model.clusterCenters()
print(centers)
#%%
model.transform(final_df).select('scaledFeatures', 'prediction').show()
#%%
