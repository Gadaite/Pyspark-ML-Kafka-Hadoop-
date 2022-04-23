from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.driver.host","192.168.1.10")\
    .config("spark.ui.showConsoleProgress","false").master("local[*]")\
    .appName("ShowSet").enableHiveSupport().getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
functions = spark.sql("set")
# functions.filter(functions["key"] == "spark.app.id").show()
functions.createOrReplaceTempView("SET")
functions.show()
spark.sql("""
    select * from SET where key like "%sql%"
""").show()
# functions.show()