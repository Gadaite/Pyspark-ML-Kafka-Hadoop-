from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.driver.host","192.168.1.10")\
    .config("spark.ui.showConsoleProgress","false").appName("BisectingKMeans")\
    .master("local[*]").getOrCreate()
from pyspark.ml.linalg import Vectors
data = [(Vectors.dense([0.0, 0.0]),),
        (Vectors.dense([1.0, 1.0]),),
        (Vectors.dense([9.0, 8.0]),),
        (Vectors.dense([8.0, 9.0]),)]
df = spark.createDataFrame(data, ["features"])
df.show()
df.printSchema()
from pyspark.ml.clustering import BisectingKMeans
bisectingKMeans = BisectingKMeans(k=2,minDivisibleClusterSize=1.0)
model = bisectingKMeans.fit(df)
print(model.clusterCenters())
print(model.computeCost(df))
print(model.hasSummary)
summary = model.summary
print(summary.clusterSizes)
print(summary.k)
print(summary.cluster)
print(summary.numIter)
print(summary.predictionCol)
summary.predictions.show()
# summary.predictions.show()
