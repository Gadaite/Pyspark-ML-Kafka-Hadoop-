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
hdfs_df = spark.read.csv("hdfs://192.168.1.10:9000/HadoopFileS/DataSet/MLdataset/College.csv,inferSchema=True,header=True")
