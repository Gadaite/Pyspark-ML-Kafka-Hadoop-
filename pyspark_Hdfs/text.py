from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.driver.host","192.168.1.10")\
    .config("spark.ui.showConsoleProgress","false")\
    .appName("text").master("local[*]").getOrCreate()
sc = spark.sparkContext
rdd = sc.textFile("hdfs://192.168.1.10:9000/HadoopFileS/DataSet/MLdataset/College.csv")
df = spark.read.csv("hdfs://192.168.1.10:9000/HadoopFileS/DataSet/titanic/test.csv",inferSchema=True,header=True)
# df = df.withColumnRenamed(df.columns[0],"College")
df.show()
df.createOrReplaceTempView("test")
spark.sql("""
    select * from test where `PassengerId` = '1310'
""").show()
df.printSchema()
print(df.count())

# import time
# time.sleep(10000000)