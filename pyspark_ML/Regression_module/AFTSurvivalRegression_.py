from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.driver.host","192.168.1.10")\
    .config("spark.ui.showConsoleProgress","false")\
    .appName("AFTSurvivalRegression").master("local[*]").getOrCreate()
from pyspark.ml.linalg import Vectors
df = spark.createDataFrame([
    (1.0, Vectors.dense(1.0), 1.0),
    (1e-40, Vectors.sparse(1, [], []), 0.0)], ["label", "features", "censor"])
df.show()
df.printSchema()

from pyspark.ml.regression import AFTSurvivalRegression
aftsr = AFTSurvivalRegression()
model = aftsr.fit(df)
model.transform(df).show()

print(model.predict(Vectors.dense(6.3)))

print(model.predictQuantiles(Vectors.dense(6.3)))
