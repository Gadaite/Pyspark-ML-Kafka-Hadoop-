#%%
from functools import update_wrapper
from io import StringIO
from itertools import count
from sys import setprofile
from pyspark.sql import SparkSession
from pyspark import SparkConf,SparkContext, conf
from pyspark.sql.types import Row, StringType, StructField
from pyspark.sql.types import * 

spark = SparkSession.builder.appName("app").master("local[*]").getOrCreate()
sc = spark.sparkContext
#%%封装连接为一个函数
def readtable(ip,post,database,table,user,password):
    jdbcDF = spark.read.format("jdbc")\
        .option("user",user)\
        .option("driver","com.mysql.jdbc.Driver")\
        .option("password",password)\
        .option("dbtable",table)\
        .option("url","jdbc:mysql://{}:{}/{}".format(ip,post,database))\
        .load()
    return jdbcDF
#%%查询
jdbcDF = readtable("139.155.70.177","3306","Gadaite","Orders","root","zzjz123")
jdbcDF.show()
#%%创建临时表
jdbcDF.createOrReplaceTempView("temp_table")
#%%测试查询结果
spark.sql("""
    select * from temp_table limit 1
    """).show()
#%%表格列名
cols = jdbcDF.columns
structlist = []
for labelname in cols:
    structlist.append(StructField(labelname,StringType()))

print(structlist)

#%%输入表结构


schemas = StructType(structlist)
#%%创建插入数据的df，并注册为临时表
rdd_add = sc.parallelize(Row(["30003","2021-11-18 14:11:05","1000000018"]))
indf = spark.createDataFrame(rdd_add,schemas)
#%%增加数据不重复的情况
insertdf = jdbcDF.unionAll(indf)
insertdf.show()

#%%删除数据的情况通过where语句操作临时表实现删除,只需要输入主键即可，假定输入的主键为order_num
deletedf = spark.sql("""
    select * from temp_table where `order_num` != "30002"
    """)
deletedf.show()
#%%
#更新数据库中的表,先删除原有数据库中有冲突主键的内容，之后再插入新的内容，实现更新
#step1:判断表中是否存在该条数据，以order_num为主键，测试使用30000
yorndf = spark.sql("""select count(*) as count from temp_table where order_num == "30000" """) 
yorndf.show()
count = yorndf.collect()[0][0]
count
#%%
#step2:根据count的值判断是否存在，如果存在就进行更新
#键入的数据如下:        "30000","2021-11-19 16:11:05","1000000028"
listupdate = ["30000","2021-11-19 16:11:05","1000000028"]
if(count != 0):
    steponedf = spark.sql("""
        select * from temp_table where `order_num` != "30000"
        """)
    steponedf.show()
    steptwodf = spark.createDataFrame(sc.parallelize(Row(listupdate)),schemas)
    steptwodf.show()
#%%
# step3更新数据库，合并具有相同结构的两个df
updatedf = steponedf.unionAll(steptwodf)
updatedf.show()

# %%
