from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.driver.host","192.168.1.10")\
    .config("spark.ui.showConsoleProgress","false")\
    .appName("DecisionTreeRegressor").master("local[*]").getOrCreate()
from pyspark.ml.linalg import Vectors
df = spark.createDataFrame([
    (1.0, Vectors.dense(1.0)),
    (0.0, Vectors.sparse(1, [], []))]
,["label", "features"])
df.show()
df.printSchema()

from pyspark.ml.regression import DecisionTreeRegressor
dt = DecisionTreeRegressor(maxDepth=2, varianceCol="variance")
model = dt.fit(df)
model.transform(df).show()

print(model.depth)
print(model.numNodes)
print(model.featureImportances)
print(model.numFeatures)

test0 = spark.createDataFrame([(Vectors.dense(-1.0),)], ["features"])
model.transform(test0).show()

test1 = spark.createDataFrame([(Vectors.sparse(1, [0], [1.0]),)], ["features"])
model.transform(test1).show()
# import time
# time.sleep(1000000)