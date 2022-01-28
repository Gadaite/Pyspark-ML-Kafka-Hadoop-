#%%
from select import select
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF,Tokenizer
from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.driver.host","192.168.1.4")\
    .config("spark.ui.showConsoleProgress","false")\
    .master("local[*]").appName("app").getOrCreate()
sc = spark.sparkContext
#%%
traindata = spark.createDataFrame([
    (0,"hello pyspark",1.0),
    (1,"using flink",0.0),
    (2,"pyspark 2.4",1.0),
    (3,"test Mysql",0.0)
],["id","text","label"])
traindata.show()
#%%
tokenizer = Tokenizer(inputCol="text",outputCol="words")
hashingTF = HashingTF(inputCol=tokenizer.getOutputCol(),outputCol="features")
logR = LogisticRegression(maxIter=10,regParam=0.001)
pipeline = Pipeline(stages=[tokenizer,hashingTF,logR])
pipeline
#%%
model = pipeline.fit(traindata)
#%%
testdata = spark.createDataFrame([
    (4,"pyspark pipeline"),
    (5,"pipeline"),
    (6,"pyspark python"),
    (7,"julia C#")
],["id","text"])
testdata.show()
#%%
predict = model.transform(testdata)
predict.show()
#%%
outdata = predict.select("id","text","probability","prediction")
outdata.show()
#%%
for row in outdata.collect():
    tid,text,probability,prediction = row
    print("(%d.%s)  ---->>> prediction = %f,probability = %s"%(tid,text,prediction,str(probability)))
#%%
res = tokenizer.transform(traindata)
res.show()
#%%