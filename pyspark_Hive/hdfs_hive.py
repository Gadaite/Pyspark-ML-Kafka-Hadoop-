from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.driver.host","192.168.1.10")\
    .config("spark.ui.showConsoleProgress","false")\
    .appName("hdfs_hive").master("local[*]").enableHiveSupport().getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
spark.sql("""
    use hive_test_one 
""")
spark.sql("""
    show tables
""").show()

# 从HDFS加载需要导入的数据
hdfs_df = spark.read.csv("hdfs://192.168.1.10:9000/HadoopFileS/DataSet/MLdataset/Customers.csv",inferSchema=True,header=True)
print(hdfs_df.count())
hdfs_df.printSchema()
# 创建hive表结构
spark.sql("""
    create table IF NOT EXISTS Customers(
        Dummy_Id string ,
        Email string ,
        Address string,
        Avatar double ,
        Avg_Session_Length double ,
        Time_on_App double,
        Time_on_Website double ,
        Length_of_Membership double ,
        Yearly_Amount_Spent string 
    )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
""")
spark.sql("show tables").show()
spark.sql("select * from Customers").show()

# 注册为临时表，并查看数据，以待导入
hdfs_df.createOrReplaceTempView("hdfs_df")
spark.sql("select * from hdfs_df").show()
# 数据overwrite的方式导入到hive表中
spark.sql("""
    insert into Customers select * from hdfs_df
""")
spark.sql("select * from Customers").show()
# 删除hive中的表
spark.sql("drop table Customers")
spark.sql("show tables").show()

