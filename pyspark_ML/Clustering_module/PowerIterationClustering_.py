from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.driver.host","192.168.1.10")\
    .config("spark.ui.showConsoleProgress","false").appName("PowerIterationClustering")\
    .master("local[*]").getOrCreate()
data = [(1, 0, 0.5),
        (2, 0, 0.5), (2, 1, 0.7),
        (3, 0, 0.5), (3, 1, 0.7), (3, 2, 0.9),
        (4, 0, 0.5), (4, 1, 0.7), (4, 2, 0.9), (4, 3, 1.1),
        (5, 0, 0.5), (5, 1, 0.7), (5, 2, 0.9), (5, 3, 1.1), (5, 4, 1.3)]
df = spark.createDataFrame(data).toDF("src", "dst", "weight")
df.show()
df.printSchema()

from pyspark.ml.clustering import PowerIterationClustering
pic = PowerIterationClustering(k=2, maxIter=40, weightCol="weight")
assignments = pic.assignClusters(df)
assignments.show()
assignments.sort(assignments.id).show(truncate=False)