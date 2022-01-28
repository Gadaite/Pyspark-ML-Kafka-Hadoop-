#%%
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
spark = SparkSession.builder.config("spark.driver.host","192.168.1.4")\
    .config("spark.ui.showConsoleProgress","false")\
    .appName("seed").master("local[*]").getOrCreate()
#%% 
data = spark.read.csv("/mnt/e/win_ubuntu/Code/DataSet/MLdataset/seeds_dataset.csv",header=True,inferSchema=True)
data.show(3)
data.printSchema()
#%%
data.columns
#%%
from pyspark.ml.feature import StringIndexer
stringIndexer = StringIndexer(inputCol="seedType",outputCol="newseedType")
model = stringIndexer.fit(data)
data = model.transform(data)
data.show(3)
#%%
data.columns
#%%
data = data.select('area','perimeter','compactness','lengthOfKernel','widthOfKernel'\
    ,'asymmetryCoefficient','lengthOfKernelGroove','newseedType')\
    .withColumnRenamed("newseedType","seedType")
#%%
vectorAssembler = VectorAssembler(inputCols=['area','perimeter','compactness',\
    'lengthOfKernel','widthOfKernel','asymmetryCoefficient',\
    'lengthOfKernelGroove'],outputCol="features")
tansdata = vectorAssembler.transform(data).select("features","seedType")
tansdata.show(3)
#%%
tansdata.head(3)
#%%
from pyspark.ml.feature import StandardScaler
standardScaler = StandardScaler(inputCol="features",outputCol="scaledFeatures")
model_one = standardScaler.fit(tansdata)
stddata = model_one.transform(tansdata)
stddata.show(3)
stddata.head(3)
#%%
traindata,testdata = stddata.randomSplit([0.7,0.3])
print(traindata.count(),testdata.count())
#%%
data.groupBy("seedType").count().show()
#%%
kmeans = KMeans(featuresCol="scaledFeatures",k=3)
model_two = kmeans.fit(traindata)
#%%
model_two.computeCost(traindata)
# %%
model_two.clusterCenters()
#%%
model_two.transform(testdata).show()
#%%
model_two.transform(traindata).show()
#%%
model_two.transform(traindata).printSchema()
#%%
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
def func(x):
    if x==1:
        return 0.0
    elif x==2:
        return 1.0
    else :
        return 2.0
fudf = udf(func,DoubleType())
#%%
trainres = model_two.transform(traindata)
testres = model_two.transform(testdata)
#%%
trainres = trainres.withColumn("res",fudf("prediction"))
trainres.show()
#%%
trainres.count()
#%%
trainres.filter(trainres.res == trainres.seedType).count()
#%%
testres = testres.withColumn("res",fudf("prediction"))
testres.count()
#%%
testres.filter(testres.res == testres.seedType).count()
testres.count()
#%%

