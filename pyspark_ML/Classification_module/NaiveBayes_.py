from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.driver.host","192.168.1.10")\
    .config("spark.ui.showConsoleProgress","false").appName("NaiveBayes")\
    .master("local[*]").getOrCreate()
from pyspark.sql import Row
from pyspark.ml.linalg import Vectors
df = spark.createDataFrame([
    Row(label=0.0, weight=0.1, features=Vectors.dense([0.0, 0.0])),
    Row(label=0.0, weight=0.5, features=Vectors.dense([0.0, 1.0])),
    Row(label=1.0, weight=1.0, features=Vectors.dense([1.0, 0.0]))])
df.show()
df.printSchema()
from pyspark.ml.classification import NaiveBayes
nb = NaiveBayes(smoothing=1.0, modelType="multinomial", weightCol="weight")
model = nb.fit(df)
model.transform(df).show()
print(model.transform(df).head(3))
print(model.pi)
print(model.theta)
# print(model.getSmoothing())
