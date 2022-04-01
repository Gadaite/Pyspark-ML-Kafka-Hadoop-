from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
spark = SparkSession.builder.config("spark.driver.host","192.168.1.10")\
    .config("spark.ui.showConsoleProgress","false").appName("KafkaFromHdfs")\
    .master("local[*]").enableHiveSupport().getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc,5)
# 使用hive数据库
spark.sql("use hive_test_one")
spark.sql("""
    create table if not exists kafkaTest(
        `ID` int,
        `area` double,
        `perimeter` double,
        `compactness` double,
        `lengthOfKernel` double,
        `widthOfKernel` double,
        `asymmetryCoefficient` double,
        `lengthOfKernelGroove` double,
        `seedType` int
    )
    row format delimited fields terminated by ','
""")
def func(x):
    for i in x:
        str = "insert into kafkaTest values(%s)"%(i)
        # print(str) 查看sql语句内容
        spark.sql(str)
    spark.sql("select * from kafkaTest").summary().show()
stream_rdd = KafkaUtils.createDirectStream(ssc,["sparkapp"],{"metadata.broker.list": "192.168.1.10:9092"})\
        .map(lambda x:x[1])
stream_rdd.foreachRDD(lambda x: func(x.collect()))
ssc.start()
ssc.awaitTermination()
property = property()


