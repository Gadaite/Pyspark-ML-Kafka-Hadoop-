from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.driver.host","192.168.1.10")\
    .config("spark.ui.showConsoleProgress","false")\
    .appName("LinearRegression").master("local[*]").getOrCreate()
from pyspark.ml.linalg import Vectors
df = spark.createDataFrame([
    (1.0, 2.0, Vectors.dense(1.0)),
    (0.0, 2.0, Vectors.sparse(1, [], []))], ["label", "weight", "features"])
df.show()
df.printSchema()
from pyspark.ml.regression import LinearRegression
lr = LinearRegression(maxIter=5, regParam=0.0, solver="normal", weightCol="weight")
model = lr.fit(df)
model.transform(df).show()
