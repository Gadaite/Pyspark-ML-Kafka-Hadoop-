from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.driver.host","192.168.1.10")\
    .config("spark.ui.showConsoleProgress","false").appName("wordcount")\
    .master("local[*]").enableHiveSupport().getOrCreate()
sc = spark.sparkContext
rdd_words = sc.textFile("hdfs://192.168.1.10:9000/user/hive/warehouse/hive_test_one.db/*")
print(rdd_words.getNumPartitions())
result = rdd_words.flatMap(lambda lines :lines.split(" "))\
                .map(lambda word : (word,1)).reduceByKey(lambda x,y : x+y).sortBy(lambda x:x[1])
result.foreach(lambda x:print(x))
sc.stop()
spark.stop()