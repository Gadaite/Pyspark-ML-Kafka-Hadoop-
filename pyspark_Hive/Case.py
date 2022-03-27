from pyspark.sql import SparkSession
import time
spark = SparkSession.builder.config("spark.driver.host","192.168.1.10")\
    .config("spark.ui.showConsoleProgress","false").master("local[*]")\
    .appName("Case").enableHiveSupport().getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
#   加载mysql数据库表
mysqldf = spark.read.format("jdbc")\
    .option("url","jdbc:mysql://192.168.1.10:3306/Gadaite")\
    .option("driver","com.mysql.cj.jdbc.Driver")\
    .option("dbtable","DJIA_table")\
    .option("user","root")\
    .option("password","LYP809834049")\
    .load()
mysqldf.show(10)
mysqldf.printSchema()

#   待加载数据注册为临时表
mysqldf.createOrReplaceTempView("mysqldf")

#   导入到hive数据库中
spark.sql("""
    use hive_test_one
""")
spark.sql("""
    create table if not exists DJIA_table(
        Dete varchar(1024),
        Open double,
        High double,
        Low double,
        Close double,
        Volume int,
        Adj_Close double
    )
    row format delimited fields terminated by ","
""")
spark.sql("""show tables""").show() #这里表的名称显示却为小写的djia_table，而我们创建的表是DJIA_table
spark.sql("""insert overwrite table DJIA_table select * from mysqldf""") #但是这里也是支持大写表名去查询
spark.sql("""select * from DJIA_table limit 10""").show()#字段显示倒是和创建的时候一样，区分大小写

#   插入数据试试是否区分大小写，使用小写的表名


# spark.sql("""
#     INSERT INTO hive_test_one.djia_table (`dete`, `open`, `high`, `low`,`close`, `volume`, `adj_close`) VALUES('2022-03-27', 0, 0, 0, 0, 0, 0)
# """)#----------------这里执行报错----------------

spark.sql("""INSERT INTO hive_test_one.djia_table 
VALUES('2022-03-27', 0, 0, 0, 0, 0, 0)
""") #这里执行正确
spark.sql("""select * from djia_table where Dete = '2022-03-27'""").show()
#   下面使用大小写的表名，运行通过，说明spark对hive表名也不区分大小写
spark.sql("""insert overwrite table djia_table select * FROM DJIA_table where Dete != '2022-03-27'""")#不能使用delete from 的方式进行删除
spark.sql("""select * from djia_table where Dete = '2022-03-27'""").show()
spark.sql("""select `dete`,`Dete` from djia_table limit 3""").show()#正常运行，说明spark对hive字段也不区分大小写
#   暂停查看spark的WebUI界面
time.sleep(60)