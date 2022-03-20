from pyspark.sql import SparkSession
import time
spark = SparkSession.builder.config("spark.driver.host","192.168.1.10")\
    .config("spark.ui.showConsoleProgress","false").appName("insert").master("local[*]")\
    .getOrCreate()
df = spark.createDataFrame([[1,2,3,4,5],[6,7,8,9,10]],["c1","c2","c3","c4","c5"])
df.show()
df.printSchema()
cols = df.columns
print(cols)
df.createOrReplaceTempView("tb")
spark.sql("select * from tb").show()
i1 = spark.createDataFrame([[11,12,13,14,15]],cols)
i1.createOrReplaceTempView("tbs")
i1.show()
spark.sql("""
    select * from tb 
    union 
    select * from tbs
""").show()
# time.sleep(1000)