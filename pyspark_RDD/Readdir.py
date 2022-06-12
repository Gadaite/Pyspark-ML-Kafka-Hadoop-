from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
spark = SparkSession.builder.config("spark.driver.host","192.168.1.10")\
    .config("spark.ui.showConsoleProgress","false")\
    .appName("readir").master("local[*]").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
path ="hdfs://192.168.1.10:9000/HadoopFileS/DataSet/MLdataset/hack_data.csv"
dfori = spark.read.csv(path,inferSchema=True,header=True)
def function():
    return path
udf_function = F.udf(function,StringType())
dfres = dfori.withColumn("Path",udf_function())
dfres.show()
# res = sc.textFile(path).map(lambda x:(x,path))
# print(res.collect()[0])