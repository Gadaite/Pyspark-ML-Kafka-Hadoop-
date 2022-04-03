
# truncate table kafkatest:删除kafkatest表中的数据但是不删除表的结构

from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
spark = SparkSession.builder.config("spark.driver.host","192.168.1.10")\
    .config("spark.ui.showConsoleProgress","false").appName("truncate_table")\
    .master("local[*]").enableHiveSupport().getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
spark.sql("show databases").show()
spark.sql("use hive_test_one")
spark.sql("show tables").show()
spark.sql("select * from kafkatest limit 10").show()
spark.sql("truncate table kafkatest")
spark.sql("select * from kafkatest limit 10").show()
ssc = StreamingContext(sc,1)
ssc.awaitTermination()