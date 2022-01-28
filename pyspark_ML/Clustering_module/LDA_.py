from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.driver.host","192.168.1.10")\
    .config("spark.ui.showConsoleProgress","false").appName("LDA")\
    .master("local[*]").getOrCreate()
from pyspark.ml.linalg import Vectors, SparseVector
df = spark.createDataFrame([
    [1, Vectors.dense([0.0, 1.0])],
    [2, SparseVector(2, {0: 1.0})],
], ["id", "features"])
df.show()
df.printSchema()

from pyspark.ml.clustering import LDA
lda = LDA(k=2, seed=1, optimizer="em")
model = lda.fit(df)
model.transform(df).show()
print(model.transform(df).head(2))

print(model.isDistributed())
localModel = model.toLocal()
print(localModel.isDistributed())

print(model.vocabSize())
print(model.topicsMatrix())
model.describeTopics().show()
print(model.describeTopics().head(2))