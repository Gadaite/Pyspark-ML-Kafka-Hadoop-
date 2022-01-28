#%%
from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.driver.host","192.168.1.4")\
    .config("spark.ui.showConsoleProgress","false")\
    .appName("BinaryClassificationEvaluator").master("local[*]").getOrCreate()
#%%
lst1 = [i for i in range(-200,200)]
lst2 = [3*i for i in range(-60,340)]
def func(x,y):
    if x>=y:
        return int(1)
    else:
        return int(0)
lst = [[lst1[i],lst2[i],func(lst1[i],lst2[i])] for i in range(0,len(lst1))]
lst
#%%
dataori = spark.createDataFrame(lst,["A","B","RES"])
dataori.show(5)
#%%
from pyspark.sql import functions
datatemp = dataori.withColumn('rand',functions.rand(seed = 42))
datatemp = datatemp.orderBy(datatemp.rand)
data = datatemp.drop(datatemp.rand)
data.show(5)
#%%
from pyspark.ml.feature import VectorAssembler
vectorAssembler = VectorAssembler(inputCols=["A","B"],outputCol="DenseVector")
data = vectorAssembler.transform(data).select("DenseVector","RES")
data.show()
#%%
traindata,testdata = data.randomSplit([0.7,0.3])
print(traindata.count(),testdata.count())
#%%
from pyspark.ml.classification import DecisionTreeClassifier
decisionTreeClassifier = DecisionTreeClassifier(featuresCol="DenseVector",labelCol="RES")
demodel = decisionTreeClassifier.fit(traindata)
resdata = demodel.transform(testdata)
#%%
resdata.show(10)
#%%
from pyspark.ml.evaluation import BinaryClassificationEvaluator
Binary_Evaluator = BinaryClassificationEvaluator(labelCol="RES")
Binary_Evaluator.evaluate(resdata)
#%%
demodel.featureImportances