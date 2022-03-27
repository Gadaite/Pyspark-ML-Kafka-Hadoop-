from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.driver.host","192.168.1.10")\
    .config("spark.ui.showConsoleProgress","false").appName("Sample")\
    .master("local[*]").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
rdd = sc.parallelize([i for i in range(0,10)])
print(rdd.getNumPartitions())
rdd.sample(False,0.5,seed=1).foreach(print)
