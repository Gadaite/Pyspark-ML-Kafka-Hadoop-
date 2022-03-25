from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.driver.host","192.168.1.10")\
    .config("spark.ui.showConsoleProgress","false")\
    .appName("tmptable").master("local[*]").enableHiveSupport().getOrCreate()
spark.sql("use hive_test_one")
spark.sql("show tables").show()