#%%
from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.Driver.host","192.168.1.4")\
    .config("spark.ui.showConsoleProgress","false").appName("app").master("local[*]")\
    .getOrCreate()
#%%
sc = spark.sparkContext
#%%
traindata = spark.read.csv('/mnt/e/win_ubuntu/Code/DataSet/titanic/train.csv',header=True,inferSchema=True).cache()
traindata.printSchema()
#%%
traindata.count()
#%%
len(traindata.columns)
#%%
traindata.show()
#%%
testdata = spark.read.csv("/mnt/e/win_ubuntu/Code/DataSet/titanic/test.csv",header=True,inferSchema=True).cache()
testdata.count()
#%%
traindata.columns
#%%
traindata.describe('Age','Pclass','SibSp','Parch').show()
#%%
traindata.describe('Sex','Cabin','Embarked','Fare','Survived').show()
#%%
traindata.groupBy("Sex","Survived").agg({'PassengerId':'count'}).show()
#%%
traindata.groupBy("Sex","Survived").agg({'PassengerId':'count'}).withColumnRenamed('count(PassengerId)',"count").show()
#%%
pdf = traindata.groupBy("Sex","Survived").agg({'PassengerId':'count'}).\
    withColumnRenamed('count(PassengerId)',"count").\
    orderBy("Sex").toPandas()
pdf

#%%
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
#%%
width = 0.35
fig,ax = plt.subplots()
labels = ["female","male"]
male_s = pdf[pdf["Survived"] == 1]["count"]
female_s = pdf[pdf["Survived"] == 0]["count"]
ax.bar(labels,male_s,width,label='Survived')
ax.bar(labels,female_s,width,bottom = male_s,label = "UnSurvived")
ax.set_ylabel("Sex")
ax.set_title("About Sex or not?")
ax.legend()
plt.show()


#%%
