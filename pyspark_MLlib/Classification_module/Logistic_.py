from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
spark = SparkSession.builder.config("spark.driver.host","192.168.1.10")\
    .config("spark.ui.showConsoleProgress","false").appName("Logistic_Method")\
    .master("local[*]").enableHiveSupport().getOrCreate()
sc = spark.sparkContext
ssc = StreamingContext(sc,1)
sc.setLogLevel("ERROR")
data_RDD = sc.textFile("hdfs://192.168.1.10:9000//HadoopFileS/DataSet/ML-100K/train.tsv")\
    .map(lambda x:x.split("\t"))
# data_RDD.foreach(print)
tmp1df = spark.createDataFrame(data_RDD)

# 提取表的字段列表
columns = [i[1:-1] for i in tmp1df.head(1)[0]]

# 提取需要的内容
rows = tmp1df.collect()
tmp2df = spark.createDataFrame(rows,schema=columns)
tmp3df = tmp2df.filter(tmp2df.url !='"url"' )

# 删除数据中的“符号
def trime(x):
    return x[1:-1]
udf_trime = udf(trime,StringType())
for i in columns:
    tmp3df = tmp3df.withColumn("RES"+i,udf_trime(i))
# tmp3df[]


ssc.awaitTermination()
