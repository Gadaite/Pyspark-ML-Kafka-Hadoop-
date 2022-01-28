#%%
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("SQLTransformer").master("local[*]").getOrCreate()
df = spark.createDataFrame([(0, 1.0, 3.0), (2, 2.0, 5.0)], ["id", "v1", "v2"])
df.show()
#%%
from pyspark.ml.feature import SQLTransformer
sqlTrans = SQLTransformer(
    statement=\
    "SELECT *, (v1 + v2) AS ADD, (v1 * v2) AS TAKE FROM __THIS__")
sqlTrans.transform(df).show()
#%%
df.createOrReplaceTempView("table")
sqlTrans = SQLTransformer(
    statement=\
    "SELECT *, (v1 + v2) AS ADD, (v1 * v2) AS TAKE FROM table")
sqlTrans.transform(df).show()
#%%
spark.stop()