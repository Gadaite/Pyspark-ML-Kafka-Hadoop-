from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.driver.host","192.168.1.10")\
    .config("spark.ui.showConsoleProgress","false").appName("OneVsRest")\
    .master("local[*]").getOrCreate()
hdfs_path = "hdfs://192.168.1.10:9000//HadoopFileS/builtInSpark/sample_multiclass_classification_data.txt"
df = spark.read.format("libsvm").load(hdfs_path)
df.show()
df.printSchema()
print(df.head(2))
from pyspark.ml.classification import LogisticRegression,OneVsRest
lr = LogisticRegression(regParam=0.01)
ovr = OneVsRest(classifier=lr)
model = ovr.fit(df)
print(len(model.models))
print([model.models[i].coefficients for i in range(0, len(model.models))])
print([model.models[i].intercept  for i in range(0, len(model.models))])

sc = spark.sparkContext
from pyspark.sql.types import Row
from pyspark.ml.linalg import Vectors
test0 = sc.parallelize([Row(features=Vectors.dense(-1.0, 0.0, 1.0, 1.0))]).toDF()
model.transform(test0).show()