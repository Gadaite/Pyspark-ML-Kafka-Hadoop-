#%%
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("VectorSizeHint").master("local[*]").getOrCreate()
from pyspark.ml.linalg import Vectors
from pyspark.ml import Pipeline, PipelineModel
data = [(Vectors.dense([1., 2., 3.]), 4.)]
df = spark.createDataFrame(data, ["vector", "float"])
df.show()
#%%
from pyspark.ml.feature import VectorSizeHint,VectorAssembler
sizeHint = VectorSizeHint(inputCol="vector", size=3, handleInvalid="skip")
vecAssembler = VectorAssembler(inputCols=["vector", "float"], outputCol="assembled")
pipeline = Pipeline(stages=[sizeHint, vecAssembler])
pipelineModel = pipeline.fit(df)
pipelineModel.transform(df).show()
#%%
sizeHint.setParams(size=2)
sizeHint.transform(df).show()
#%%
spark.stop()