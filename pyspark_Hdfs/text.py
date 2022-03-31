from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.driver.host","192.168.1.10")\
    .config("spark.ui.showConsoleProgress","false")\
    .appName("text").master("local[*]").getOrCreate()
sc = spark.sparkContext
rdd = sc.textFile("hdfs://192.168.1.10:9000/HadoopFileS/DataSet/MLdataset/College.csv")
# print(rdd.collect()[0]
# other = spark.read.parquet("hdfs://192.168.1.10:9000/HadoopFileS/DataSet/MLdataset/College.csv")
# print(type(other))
# df  = spark.createDataFrame(rdd)
df = spark.read.csv("hdfs://192.168.1.10:9000/HadoopFileS/DataSet/MLdataset/College.csv",inferSchema=True,header=True)
df = df.withColumnRenamed(df.columns[0],"College")
df.show(3)

spark.cacheTable()
df.printSchema()
print(df.count())

# import time
# time.sleep(10000000)