from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.driver.host","192.168.1.10")\
    .config("spark.ui.showConsoleProgress","false").appName("KMeans")\
    .master("local[*]").getOrCreate()
from pyspark.ml.linalg import Vectors
data = [(Vectors.dense([0.0, 0.0]),), (Vectors.dense([1.0, 1.0]),),
        (Vectors.dense([9.0, 8.0]),), (Vectors.dense([8.0, 9.0]),)]
df = spark.createDataFrame(data, ["features"])
df.show()
df.printSchema()
from pyspark.ml.clustering import KMeans
kMeans = KMeans(k=2,seed=1)
model = kMeans.fit(df)
model.transform(df).show()
print(model.hasSummary)
print(model.clusterCenters())
print(model.summary.clusterSizes)
testdata = spark.createDataFrame([
    (Vectors.dense([8.2,8.2]),)
],["test"])
testdata.show()
# model().transform(testdata).show()
testdata = testdata.withColumnRenamed("test","features")
model.transform(testdata).show()