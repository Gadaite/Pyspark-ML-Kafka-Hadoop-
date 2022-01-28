#%%
from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.Driver.host","192.168.1.4")\
    .config("spark.ui.showConsoleProgress","false")\
    .appName("IndexToString").master("local[*]").getOrCreate()
#%%
df = spark.createDataFrame([(1,"hello"),(2,"world"),(3,"hello"),(4,"spark")]\
    ,["indexcol","text"])
df.show()
#%%
from pyspark.ml.feature import StringIndexer
stringIndexer = StringIndexer(inputCol="text",outputCol="stringindex")
model1 = stringIndexer.fit(df)
temp1df = model1.transform(df)
temp1df.show()
#%%
temp1df.printSchema()
#%%
from pyspark.ml.feature import IndexToString
indexToString = IndexToString(inputCol="stringindex",outputCol="oritext")
oridf = indexToString.transform(temp1df)
oridf.show()
#%%
spark.stop()