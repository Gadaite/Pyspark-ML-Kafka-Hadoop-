from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.driver.host","192.168.1.10")\
    .config("spark.ui.showConsoleProgress","false").appName("GaussianMixture")\
    .master("local[*]").getOrCreate()
from pyspark.ml.linalg import Vectors
data = [(Vectors.dense([-0.1, -0.05]),),
        (Vectors.dense([-0.01, -0.1]),),
        (Vectors.dense([0.9, 0.8]),),
        (Vectors.dense([0.75, 0.935]),),
        (Vectors.dense([-0.83, -0.68]),),
        (Vectors.dense([-0.91, -0.76]),)]
df = spark.createDataFrame(data, ["features"])
df.show()
df.printSchema()
from pyspark.ml.clustering import GaussianMixture
gaussianMixture = GaussianMixture(k=3,maxIter=10,tol=0.0001,seed=10)
model = gaussianMixture.fit(df)
print(model.transform(df).collect())
print(model.hasSummary)
print(model.weights)
print(model.summary.k)
model.gaussiansDF.show()
print(model.gaussiansDF.head(6))
print(model.summary.clusterSizes)
print(model.summary.logLikelihood)


