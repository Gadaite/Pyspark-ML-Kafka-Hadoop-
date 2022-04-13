from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
spark = SparkSession.builder.config("spark.driver.host","192.168.1.10")\
    .config("spark.ui.showConsoleProgress","false").appName("readsocketdata")\
    .master("local[*]").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc,10)
socketdata = ssc.socketTextStream("192.168.1.10",7777)
socketdata.foreachRDD(lambda x:print(x.collect()))
ssc.start()
ssc.awaitTermination()