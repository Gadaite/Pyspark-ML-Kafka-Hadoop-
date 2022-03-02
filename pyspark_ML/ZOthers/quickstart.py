#%%
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
#%%
# 创建具有两个工作线程和1秒批处理间隔的本地StreamingContext
sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 1)
#%%
# 创建一个将连接到hostname:port的数据流，如localhost:9999
lines = ssc.socketTextStream("127.0.0.1", 3306)
#%%
# 把每行分成多个词
words = lines.flatMap(lambda line: line.split(" "))
#%%
# 计算每批中的每个单词
pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)
#%%
# 将此数据流中生成的每个RDD的元素打印到控制台
wordCounts.pprint()
#%%
ssc.start()             # 开始计算
ssc.awaitTermination()  # 等待计算结束
#%%

#%%
