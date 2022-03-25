from pyspark.sql import SparkSession
import datetime
spark = SparkSession.builder.config("spark.driver.host","192.168.1.10")\
    .config("spark.ui.showConsoleProgress","false").appName("Mysql_Hive")\
    .master("local[*]").enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
t1 = datetime.datetime.now()
mysqldf = spark.read.format("jdbc")\
    .option("driver","com.mysql.cj.jdbc.Driver")\
    .option("url","jdbc:mysql://192.168.1.10:3306/Gadaite")\
    .option("dbtable","RedditNews")\
    .option("user","root")\
    .option("password","LYP809834049")\
    .load()
print(mysqldf.head(1))
mysqldf.printSchema()
mysqldf.createOrReplaceTempView("mysqldf")

spark.sql("use hive_test_one")
spark.sql("""
    create table if not exists RedditNews(
        Data string,
        News String
    )
    row format delimited fields terminated by ','
""")
spark.sql("""
    insert overwrite table RedditNews select * from mysqldf
""")
t2 = datetime.datetime.now()
spark.sql("show tables").show()
spark.sql("select count(*) from RedditNews").show()

print((t2 - t1).seconds)