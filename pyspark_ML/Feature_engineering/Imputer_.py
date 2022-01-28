#%%
from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.Driver.host","192.168.1.4")\
    .config("spark.ui.showConsoleProgress","false")\
    .appName("Imputer").master("local[*]").getOrCreate()
#%%
df = spark.createDataFrame([(1.0, float("nan")), (2.0, float("nan")), (2.0, float("nan"))\
    , (float("nan"), 3.0),(4.0, 4.0), (5.0, 5.0)], ["a", "b"])
df.show()
#%%
from pyspark.ml.feature import Imputer
imputer = Imputer(inputCols=["a", "b"], outputCols=["out_a", "out_b"])
model = imputer.fit(df)
model.transform(df).show()
#%%
imputer2 = Imputer(strategy="median",inputCols=["a", "b"], outputCols=["out_a", "out_b"])
model2 = imputer2.fit(df)
model2.transform(df).show()
#%%
model.surrogateDF.show()
#%%
model.surrogateDF.show()