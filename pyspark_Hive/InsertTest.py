from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.driver.host","192.168.1.10")\
    .config("spark.ui.showConsoleProgress","false").appName("InsertTest")\
    .master("local[*]").enableHiveSupport().getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
lst = ['1,15.26,14.84,0.871,5.763,3.312,2.221,5.22,1', '2,14.88,14.57,0.8811,5.554,3.333,1.018,4.956,1']
spark.sql("use hive_test_one")
spark.sql("select * from kafkaTest").show()
def func(x):
    for i in x:
        str = "insert into kafkaTest values(%s)"%(i)
        print(str)
        spark.sql(str)
func(lst)
spark.sql("select * from kafkaTest").show()
spark.sql("drop table kafkaTest")
spark.sql("show tables").show()