from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.driver.host","192.168.1.10")\
    .config("spark.ui.showConsoleProgress","false")\
    .appName("IsotonicRegression").master("local[*]").getOrCreate()
from pyspark.ml.linalg import Vectors
df = spark.createDataFrame([
    (1.0, Vectors.dense(1.0)),
    (0.0, Vectors.sparse(1, [], []))], ["label", "features"])
df.show()
from pyspark.ml.regression import IsotonicRegression
ir = IsotonicRegression()
model = ir.fit(df)
model.transform(df).show()
test0 = spark.createDataFrame([(Vectors.dense(-1.0),)], ["features"])
print(model.transform(test0).head())
print(model.boundaries)