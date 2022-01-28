#%%
from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.Driver.host","192.168.1.3")\
    .config("spark.ui.showConsoleProgress","false")\
    .appName("FeatureHasher").master("local[*]").getOrCreate()
#%%
data = [(2.0, True, "1", "foo"), (3.0, False, "2", "bar")]
cols = ["real", "bool", "stringNum", "string"]
df = spark.createDataFrame(data, cols)
df.show()
#%%
df.printSchema()
#%%
from pyspark.ml.feature import FeatureHasher
hasher = FeatureHasher(inputCols=cols, outputCol="features")
hasher.transform(df).show()
#%%
hasher.transform(df).head(2)
#%%
resdf = hasher.transform(df).toPandas()
resdf
