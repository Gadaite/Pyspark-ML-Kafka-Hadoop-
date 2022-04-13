#%%
from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.driver.host","192.168.1.5")\
    .config("spark.ui.showConsoleProgress","false")\
    .appName("Ecommerce").master("local[*]").getOrCreate()
#%%
data = spark.read.csv("/mnt/f/Code/DataSet/MLdataset/CarPrice_Assignment.csv",header=True,inferSchema=True)
#%%
data.head(1)
#%%
data.count()
#%%
colAll = data.columns
#%%
def func(x):
    return x/1000
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
fudf = udf(func,DoubleType())
data = data.withColumn("trans_price",fudf("price"))
data.head(1)
#%%
#%%
#-------------------------
zhcol = ["CarName","fueltype","aspiration"\
    ,"doornumber","carbody","drivewheel","enginelocation","enginetype"\
    ,"cylindernumber","fuelsystem"]
#%%
from pyspark.ml.feature import StringIndexer
for i in zhcol:
    stringIndexer = StringIndexer(inputCol=i,outputCol=i+"-index")
    model = stringIndexer.fit(data)
    data = model.transform(data)
#%%
print(len(colAll),len(zhcol),len(data.columns))
#%%
data.head(1)
#%%
colsAll = data.columns
for i in zhcol:
    colsAll.remove(i)
colsAll.remove("car_ID")
colsAll
#%%
len(colsAll)
#%%
#%%
data = data.select('symboling','wheelbase','carlength','carwidth'\
    ,'carheight','curbweight','enginesize','boreratio','stroke'\
    ,'compressionratio','horsepower','peakrpm','citympg','highwaympg'\
    ,'CarName-index','fueltype-index','aspiration-index','doornumber-index'\
    ,'carbody-index','drivewheel-index','enginelocation-index','enginetype-index'\
    ,'cylindernumber-index','fuelsystem-index','trans_price')
#%%
data.head(1)
#%%
colsAll
#%%
colsAll.remove("price")
colsAll.remove("trans_price")
from pyspark.sql.functions import corr
dict = {}
for i in colsAll:
    dict[i] = data.select(corr(i,"trans_price")).collect()[0][0]
#%%
sorted(dict.items(),key=lambda item:item[1])
#%%
colss = [sorted(dict.items(),key=lambda item:item[1])[i][0] for i in [0,1,-14,-13,-12,-11,-10,-9,-8,-7,-6,-5,-4,-3,-2,-1]]
colss
# colss.remove("price")
#%%
data.head(1)
#%%
from pyspark.ml.feature import VectorAssembler
vectorAssembler1 = VectorAssembler(inputCols=colss,outputCol="features")
data = vectorAssembler1.transform(data)
data.head(1)
#%%
data = data.select("features","trans_price")
data.head(1)
#%%
from pyspark.ml.feature import StandardScaler
standardScaler = StandardScaler(inputCol="features",outputCol="stdScaler")
model_std = standardScaler.fit(data)
data = model_std.transform(data)
#%%
data.head(1)
#%%
traindata,testdata = data.randomSplit([0.7,0.3])
print(traindata.count(),testdata.count())
#%%
from pyspark.ml.regression import LinearRegression
linearRegression = LinearRegression(featuresCol="stdScaler",labelCol="trans_price")
model = linearRegression.fit(traindata)
traindata = model.transform(traindata)
traindata.show(10)
#%%
testresult = model.evaluate(testdata)
#%%
testresult.rootMeanSquaredError
#%%
testresult.r2
#%%
testresult.meanSquaredError
#%%
testresult.meanAbsoluteError
#%%
testdata = model.transform(testdata)
testdata.show(15)
#%%
spark.stop()