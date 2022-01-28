#%%
from pyspark.sql import SparkSession
from pyspark.ml.classification import DecisionTreeClassifier,RandomForestClassifier,GBTClassifier
from pyspark.ml.feature import StringIndexer,VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator,MulticlassClassificationEvaluator
import pandas as pd
#%%
spark = SparkSession.builder.config("spark.driver.host","192.168.1.4")\
    .config("spark.ui.showConsoleProgress","false")\
    .appName("DecisionTreeAndRandomforest").master("local[*]").getOrCreate()
#%%
data = spark.read.csv("/mnt/e/win_ubuntu/Code/DataSet/MLdataset/College.csv",header=True,inferSchema=True)
#%%
data = data.withColumnRenamed(data.columns[0],"School")
#%%
data.head(1)
#%%
data.columns
#%%
data.count()
# %%
stringIndexer = StringIndexer(inputCol="Private",outputCol="index-Private")
model = stringIndexer.fit(data)
stringindexdata = model.transform(data)
stringindexdata.head(1)
#%%
stringindexdata.select("index-Private").distinct().show()
#%%
VectorCols = stringindexdata.columns
VectorCols.remove("School")
VectorCols.remove("Private")
VectorCols.remove("index-Private")
VectorCols
#%%
vectorAssembler = VectorAssembler(inputCols=VectorCols,outputCol="vectfeatures")
vectordata = vectorAssembler.transform(stringindexdata).select("vectfeatures","index-Private")
vectordata.head(1)
#%%
traindata,testdata = vectordata.randomSplit([0.7,0.3])
print(traindata.count(),testdata.count())
#%%
decisionTreeClassifier = DecisionTreeClassifier(featuresCol="vectfeatures",labelCol="index-Private")
decisionTree_model = decisionTreeClassifier.fit(traindata)
decisionTree_testdata = decisionTree_model.transform(testdata)
#%%
randomForestClassifier = RandomForestClassifier(featuresCol="vectfeatures",labelCol="index-Private")
randomForest_model = randomForestClassifier.fit(traindata)
randomForest_testdata = randomForest_model.transform(testdata)
#%%
gbtClassifier = GBTClassifier(featuresCol="vectfeatures",labelCol="index-Private")
gbt_model = gbtClassifier.fit(traindata)
gbt_testdata = gbt_model.transform(testdata)
#%%
Binary_Evaluator = BinaryClassificationEvaluator(labelCol="index-Private")
#%%
print('Decision Tree:', Binary_Evaluator.evaluate(decisionTree_testdata))
#%%
print('Random Fores:', Binary_Evaluator.evaluate(randomForest_testdata))
#%%
print('Gradient-boosted Trees:', Binary_Evaluator.evaluate(gbt_testdata))
#%%
Multi_Evaluator = MulticlassClassificationEvaluator(labelCol="index-Private",metricName="accuracy")
#%%
print('Decision Tree accuracy:', Multi_Evaluator.evaluate(decisionTree_testdata))
#%%
print('Random Fores accuracy:', Multi_Evaluator.evaluate(randomForest_testdata))
#%%
print('Gradient-boosted Trees accuracy:', Multi_Evaluator.evaluate(gbt_testdata))
#%%
decisionTree_model.featureImportances
#%%
randomForest_model.featureImportances
#%%
gbt_model.featureImportances
#%%
randomForest_model.trees
#%%
gbt_model.trees
#%%
gbt_model.evaluateEachIteration(gbt_testdata)
#%%
spark.stop()
#%%