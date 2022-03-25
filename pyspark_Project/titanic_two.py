#%%
from pyspark.sql import SparkSession
from pyspark.sql.context import SQLContext
from pyspark.ml.feature import StringIndexer,VectorAssembler,VectorIndexer
spark = SparkSession.builder.config("spark.driver.host","192.168.1.10")\
    .config("spark.ui.showConsoleProgress","false")\
    .appName("app").master("local[*]").getOrCreate()
# %%
sc = spark.sparkContext
#%%
traindata = spark.read.csv("/mnt/e/win_ubuntu/Code/DataSet/titanic/train.csv",header=True,inferSchema=True).cache()
traindata.show()
#%%
traindata.count()
#%%
len(traindata.columns)
#%%
traindata.describe("Age").show()
#%%
temp1df = traindata.describe("Age").withColumnRenamed('summary','Mthod')
temp2df = temp1df.filter(temp1df.Mthod == 'mean')
temp2df.show()
#%%
avg =round(float(temp2df.collect()[0][1]),3)
avg
#%%
traindata = traindata.fillna({"Age":avg})
traindata.show()

#%%
#%%
traindata.printSchema()
#%%
traindata.createOrReplaceTempView("viewone")
#%%
mores = spark.sql("""
    select Embarked,count(Embarked) as count from viewone 
    group by Embarked 
    order by count desc limit 1
    """).collect()[0][0]
mores
#%%
traindata = traindata.fillna({'Embarked':mores})
traindata.show()
#%%
traindata = traindata.fillna({'Cabin':"unknown"})
traindata.show()
#%%
traindata.printSchema()
#%%
traindata = traindata.drop("Cabin")
traindata = traindata.drop("Ticket")
traindata.printSchema()

#%%
labelIndexer = StringIndexer(inputCol="Embarked",outputCol="iEmbarked")
model = labelIndexer.fit(traindata)
traindata = model.transform(traindata)
#%%
labelIndexer = StringIndexer(inputCol="Sex",outputCol="iSex")
model = labelIndexer.fit(traindata)
traindata = model.transform(traindata)
#%%
traindata.show()
#%%
features = ['Pclass','iSex','Age','SibSp','Parch','Fare','iEmbarked','Survived']
traindata_features = traindata[features]
traindata_features.show()
#%%
traindata_ass = VectorAssembler(inputCols=['Pclass','iSex','Age','SibSp','Parch','Fare','iEmbarked'],outputCol='features')
ass_df = traindata_ass.transform(traindata_features)
ass_df["features",].show()
ass_df["Survived",].show()