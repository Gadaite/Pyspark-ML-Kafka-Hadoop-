#%%
from pyspark.sql import SparkSession
from pyspark.sql.types import Row
from pyspark.ml.linalg import Vectors
spark = SparkSession.builder.appName("LinearSVC")\
    .master("local[*]").getOrCreate()
sc = spark.sparkContext
#%%
df = sc.parallelize([
    Row(label=1.0, features=Vectors.dense(1.0, 1.0, 1.0)),
    Row(label=0.0, features=Vectors.dense(1.0, 2.0, 3.0))
]).toDF()
df.show()
#%%
from pyspark.ml.classification import LinearSVC
svm = LinearSVC(maxIter=5, regParam=0.01)
model = svm.fit(df)
model.transform(df).show()
#%%
model.transform(df).head(3)
#%%
model.coefficients
#%%
model.numClasses
#%%
model.numFeatures
#%%
test0 = sc.parallelize([Row(features=Vectors.dense(-1.0, -1.0, -1.0))]).toDF()
result = model.transform(test0).head()
result.prediction
#%%
result.rawPrediction

#%%
spark.stop()