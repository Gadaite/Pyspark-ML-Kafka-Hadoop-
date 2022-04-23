from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.driver.host","192.168.1.10")\
    .config("spark.ui.showConsoleProgress","false")\
    .appName("text").master("local[*]").enableHiveSupport().getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
df = spark.read.csv("hdfs://192.168.1.10:9000//SqoopData/sqpt1.csv",inferSchema=True,header=True)
df.show(3)
df.createOrReplaceTempView("test")
print(df.count())
spark.sql("use hive_test_one")
spark.sql("show tables").show()
# spark.catalog.dropTempView("test")?
spark.sql("show tables").show()
# spark.sql("select * from test limit 1").show()