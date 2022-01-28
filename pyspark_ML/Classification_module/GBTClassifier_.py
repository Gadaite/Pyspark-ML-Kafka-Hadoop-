from pyspark.sql import SparkSession
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import StringIndexer
from numpy import allclose
spark = SparkSession.builder.config("spark.driver.host","192.168.1.10")\
    .config("spark.ui.showConsoleProgress","false").appName("GBTClassifier")\
    .master("local[*]").getOrCreate()
df = spark.createDataFrame([
    (1.0, Vectors.dense(1.0)),
    (0.0, Vectors.sparse(1, [], []))], ["label", "features"])
df.show()
stringIndexer = StringIndexer(inputCol="label", outputCol="indexed")
si_model = stringIndexer.fit(df)
td = si_model.transform(df)
td.show()
gbt = GBTClassifier(maxIter=5, maxDepth=2, labelCol="indexed", seed=42)
print("--"*10)
print(gbt.getFeatureSubsetStrategy())
print("__"*10)
model = gbt.fit(td)
model.transform(td).show()
print(model.transform(td).head(2))
print(model.featureImportances)
print(model.treeWeights)
print(allclose(model.treeWeights, [1.0, 0.1, 0.1, 0.1, 0.1]))
test0 = spark.createDataFrame([(Vectors.dense(-1.0),)], ["features"])
print(model.transform(test0).head().prediction)
test1 = spark.createDataFrame([(Vectors.sparse(1, [0], [1.0]),)], ["features"])
test0.show()
test1.show()
print(model.transform(test1).head().prediction)
print(model.totalNumNodes)
print(model.toDebugString)