#%%
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("RFormula")\
    .master("local[*]").getOrCreate()
#%%
df = spark.createDataFrame([
    (1.0, 1.0, "a"),
    (0.0, 2.0, "b"),
    (0.0, 0.0, "a")
], ["y", "x", "s"])
df.show()
#%%
from pyspark.ml.feature import RFormula
rf = RFormula(formula="y ~ x + s")
model = rf.fit(df)
model.transform(df).show()
#%%
rf.fit(df, {rf.formula: "y ~ . - s"}).transform(df).show()
#%%
spark.stop()