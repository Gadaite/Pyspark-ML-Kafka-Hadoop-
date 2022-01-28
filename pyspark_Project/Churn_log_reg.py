#%%
from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
spark = SparkSession.builder.config("spark.driver.host","192.168.1.4")\
    .config("spark.ui.showConsoleProgress","false")\
    .appName("Churn_log_reg").master("local[*]").getOrCreate()
# %%
data = spark.read.csv("/mnt/e/win_ubuntu/Code/DataSet/MLdataset/customer_churn.csv",header=True,inferSchema=True)
data.printSchema()
#%%
data.columns
#%%
data.count()
#%%
data.show()
#%%
spark.createDataFrame(data.describe().toPandas().transpose().reset_index()).show()
#%%
vectorAssembler = VectorAssembler(inputCols=['Age', 'Total_Purchase', 'Account_Manager', 'Years', 'Num_Sites'],outputCol="features")
#%%
log_R = LogisticRegression(featuresCol="features",labelCol="Churn",maxIter=10)
#%%
pipline = Pipeline(stages=[vectorAssembler,log_R])
#%%
traindata,testdata = data.randomSplit([0.7,0.3])
print(traindata.count(),testdata.count())
#%%
lrmodel = pipline.fit(traindata)
predict = lrmodel.transform(testdata)
#%%
predict.printSchema()
#%%
predict.select('Churn', 'prediction', 'probability', 'rawPrediction').show(10)
#%%
from pyspark.ml.evaluation import BinaryClassificationEvaluator
eval = BinaryClassificationEvaluator(labelCol = 'Churn', rawPredictionCol = 'rawPrediction')
eval.evaluate(predict)
#%%
lrmodel.stages[1].summary.recallByLabel
#%%
predict.select("Churn","rawPrediction").show()