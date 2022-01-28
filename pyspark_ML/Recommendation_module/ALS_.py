from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.Driver.host","192.168.1.10")\
    .config("spark.ui.showConsoleProgress","false")\
    .appName("ALS").master("local[*]").getOrCreate()
df = spark.createDataFrame([
    (0, 0, 4.0), (0, 1, 2.0),
    (1, 1, 3.0), (1, 2, 4.0),
    (2, 1, 1.0), (2, 2, 5.0)]
,["user", "item", "rating"])
df.show()

from pyspark.ml.recommendation import ALS
als = ALS(rank=10, maxIter=5, seed=0)
model = als.fit(df)
model.transform(df).show()

print(model.rank)
print(type(model.userFactors))
print(model.userFactors.head(10))

test = spark.createDataFrame([(0, 2), (1, 0), (2, 0)], ["user", "item"])
print(model.transform(test).select(["prediction"]).head(10))
# import time
# time.sleep(10000000)
