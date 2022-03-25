
from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.driver.host","192.168.1.10")\
    .config("spark.ui.showConsoleProgress","false")\
    .appName("ReadHive").master("local[*]").enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("Error")
spark.sql("show databases").show()
spark.sql("use hive_test_one")
spark.sql("select * from seeds_dataset").show()
