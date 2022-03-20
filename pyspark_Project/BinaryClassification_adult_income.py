#%%
from pyspark.sql import SparkSession
spark = SparkSession.builder.config('spark.driver.host','192.168.1.10')\
    .config('spark.ui.showConsoleProgress','false')\
    .appName('BinaryClassification').master('local[*]').getOrCreate()
#%%
data = spark.read.csv("hdfs://192.168.1.10:9000/HadoopFileS/DataSet/MLdataset/adult.csv",header=True,inferSchema=True)
#%%
data.printSchema()
#%%
data.count()
#%%
data.show()
#%%
len(data.columns)
#%%
res = data.toPandas()
res
#%%
from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler
#%%
categoricalColumns = ["workclass", "education", "marital-status", "occupation", "relationship", "race", "gender", "native-country"]
stages = []

for categoricalCol in categoricalColumns:
    stringIndexer = StringIndexer(inputCol = categoricalCol, outputCol = categoricalCol + 'Index')
    encoder = OneHotEncoderEstimator(inputCols=[stringIndexer.getOutputCol()], outputCols=[categoricalCol + "classVec"])
    stages += [stringIndexer, encoder]
#%%
label_stringIdx = StringIndexer(inputCol = 'income', outputCol = 'label')
stages += [label_stringIdx]
#%%
numericCols = ["age", "fnlwgt", "educational-num", "capital-gain", "capital-loss", "hours-per-week"]
assemblerInputs = [c + "classVec" for c in categoricalColumns] + numericCols
assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
stages += [assembler]
#%%
pipeline = Pipeline(stages=stages)
pipelineModel = pipeline.fit(data)
df = pipelineModel.transform(data)
selectedcols = ["label", "features"] + data.columns
df = df.select(selectedcols)
outdf = df.toPandas()
print(outdf)
#%%
df
#%%
train, test = df.randomSplit([0.7, 0.3], seed=100)
print(train.count())
print(test.count())

#
import time
time.sleep(1000)




