#%%
from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
spark = SparkSession.builder.config("spark.driver.host","192.168.1.10")\
    .config("spark.ui.showConsoleProgress","false")\
    .appName("CruiseLinear").master("local[*]").getOrCreate()
#%%
data = spark.read.csv("hdfs://192.168.1.10:9000/HadoopFileS/DataSet/MLdataset/cruise_ship_info.csv",header=True,inferSchema=True)
data.printSchema()
#%%
data.show(3)
#%%
from pyspark.sql.functions import desc
data.groupBy('Cruise_line').count().orderBy('count').sort(desc("count")).show()
#%%
from pyspark.ml.feature import StringIndexer
stringIndexer = StringIndexer(inputCol="Cruise_line",outputCol="Cruise_category")
model_one = stringIndexer.fit(data)
data = model_one.transform(data)
data.show(3)
#%%
from pyspark.ml.feature import VectorAssembler
vectorAssembler = VectorAssembler(inputCols=['Age', 'Tonnage', 'passengers', 'length', 'cabins', 'passenger_density', 'Cruise_category']\
    ,outputCol="features")
data = vectorAssembler.transform(data)
data.show(3)
# %%
data = data.select(["features","crew"])
data.show(3)
#%%
data.head(3)
#%%
data.count()
#%%
datatrain,datatest = data.randomSplit([0.7,0.3])
print(datatrain.count(),datatest.count())
#%%
datatrain.describe().show()
#%%
datatest.describe().show()
#%%
linearRegression = LinearRegression(labelCol="crew")
model_two = linearRegression.fit(datatrain)
train_res = model_two.transform(datatrain)
train_res.show(3)
#%%
res_test = model_two.evaluate(datatest)
#%%
res_test.rootMeanSquaredError
#%%
res_test.r2
#%%
res_test.meanSquaredError
#%%
res_test.meanAbsoluteError
#%%
oridata = spark.read.csv("hdfs://192.168.1.10:9000/HadoopFileS/DataSet/MLdataset/cruise_ship_info.csv",header=True,inferSchema=True)
#%%
from pyspark.sql.functions import corr
oridata.select(corr('crew', 'passengers')).show()
#%%
oridata.select(corr('crew', 'cabins')).show()

#%%
spark.createDataFrame(oridata.describe().toPandas().transpose().reset_index()).show()

import time
time.sleep(100)