#%%
from pyspark.sql import SparkSession
from pyspark.sql.types import Row
from pyspark.ml.linalg import Vectors
spark = SparkSession.builder.appName("LogisticRegression").master("local[*]").getOrCreate()
bdf = spark.createDataFrame([
    Row(label=1.0, weight=1.0, features=Vectors.dense(0.0, 5.0)),
    Row(label=0.0, weight=2.0, features=Vectors.dense(1.0, 2.0)),
    Row(label=1.0, weight=3.0, features=Vectors.dense(2.0, 1.0)),
    Row(label=0.0, weight=4.0, features=Vectors.dense(3.0, 3.0))
])
bdf.show()
#%%
from pyspark.ml.classification import LogisticRegression
blor = LogisticRegression(regParam=0.01, weightCol="weight")
blorModel = blor.fit(bdf)
blorModel.transform(bdf).show()
#%%
blorModel.coefficients
#%%
blorModel.intercept
#%%
spark.stop()