from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

sc = SparkContext(appName="streamingkafka")
sc.setLogLevel("WARN") #设置日志级别
ssc = StreamingContext(sc, 5) # 计算时间间隔
brokers='192.168.1.10:9092' # IP：端口
topic = 'sparkapp' #kafka主题
# 使用streaming使用直连模式消费kafka
kafka_streaming_rdd = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
lines_rdd = kafka_streaming_rdd.map(lambda x: x[1])
counts = lines_rdd.flatMap(lambda line: line.split(" ")) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda a, b: a+b)
# 将workcount结果打印到当前shell
counts.pprint()
ssc.start()
ssc.awaitTermination()

property = property()