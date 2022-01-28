from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.driver.host","192.168.1.10")\
    .config("spark.ui.showConsoleProgress","false").appName("RandomForestClassifier")\
    .master("local[*]").getOrCreate()
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import StringIndexer
df = spark.createDataFrame([
    (1.0, Vectors.dense(1.0)),
    (0.0, Vectors.sparse(1, [], []))], ["label", "features"])
df.show()
stringIndexer = StringIndexer(inputCol="label", outputCol="indexed")
si_model = stringIndexer.fit(df)
td = si_model.transform(df)
td.show()
from pyspark.ml.classification import RandomForestClassifier
rf = RandomForestClassifier(numTrees=3, maxDepth=2, labelCol="indexed", seed=42)
model = rf.fit(td)
model.transform(td).show()
print(model.transform(td).head(2))
print(model.featureImportances)
print(model.treeWeights)
import numpy
print(numpy.allclose(model.treeWeights, [1.0, 1.0, 1.0]))
test0 = spark.createDataFrame([(Vectors.dense(-1.0),)], ["features"])
test0.show()
model.transform(test0).show()
print(model.transform(test0).head(1))

test1 = spark.createDataFrame([(Vectors.sparse(1, [0], [1.0]),)], ["features"])
model.transform(test1).show()
print(model.transform(test1).head(1))
print(model.totalNumNodes)
print(model.trees)
print(model.toDebugString)
