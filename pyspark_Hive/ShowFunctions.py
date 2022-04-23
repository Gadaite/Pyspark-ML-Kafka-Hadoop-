from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.driver.host","192.168.1.10")\
    .config("spark.ui.showConsoleProgress","false").master("local[*]")\
    .appName("ShowFunc").enableHiveSupport().getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
functions = spark.sql("show functions")
functionslist = functions.collect()
print(functionslist[0])
print(functionslist[0][0])
res = ""
for i in range(0,len(functionslist)):
    if(i%10!=9):
        res = res + functionslist[i][0] +","
    if(i%10==9):
        res = res + functionslist[i][0] +"\n "
print(res)
