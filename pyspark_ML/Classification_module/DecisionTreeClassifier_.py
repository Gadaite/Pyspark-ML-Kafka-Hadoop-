#%%
from pyspark.sql import SparkSession
spark = SparkSession.builder.config('spark.driver.host','192.168.1.10').\
    appName("DecisionTreeClassifier").master("local[*]").getOrCreate()
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import StringIndexer
df = spark.createDataFrame([
    (1.0, Vectors.dense(1.0)),
    (0.0, Vectors.sparse(1, [], []))], ["label", "features"])
df.show()
#%%
from pyspark.ml.classification import DecisionTreeClassifier
DecisionTreeClassifier(featuresCol="")
#%%
spark.stop()
df.printSchema()

import time
time.sleep(2000000)