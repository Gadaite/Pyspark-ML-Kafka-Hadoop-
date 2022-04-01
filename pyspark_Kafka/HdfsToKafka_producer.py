from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from kafka import KafkaProducer
from kafka.errors import KafkaError
import time
sc = SparkSession.builder.config("spark.driver.host","192.168.1.10")\
    .config("spark.ui.showConsoleProgress","false").appName("HdfsKafka")\
    .master("local[*]").getOrCreate().sparkContext
ssc = StreamingContext(sc,10)
producer = KafkaProducer(bootstrap_servers = "192.168.1.10:9092")
rdd = sc.textFile("hdfs://192.168.1.10:9000//HadoopFileS/DataSet/MLdataset/seeds_dataset.csv")
# rdd.foreach(print) 用于查看hdfs数据
print("---------------开始用hdfs数据模拟kafka生产----------------------")
lst = rdd.collect()[1:] #去除第一行的字段数据
for i in lst:
    future = producer.send(topic="sparkapp",value=i.encode())
    print("已生产数据：%s"%(i))
    time.sleep(1)
    try:
        record = future.get(timeout=10)
    except KafkaError as e:
        print(e)
ssc.start()
ssc.awaitTermination()
property = property()