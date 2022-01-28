#%%
from pyspark import SparkConf,SparkContext
from pyspark import sql
from pyspark.sql import SparkSession
spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
sc = spark.sparkContext
 
jdbcDF = spark.read.format("jdbc")\
    .option("driver","com.mysql.jdbc.Driver")\
    .option("url","jdbc:mysql://139.155.70.177:3306/Gadaite")\
    .option("dbtable","audi")\
    .option("user","root")\
    .option("password","zzjz123")\
    .load()
jdbcDF.createOrReplaceTempView("temptable")
 
#%%
#查询最大值，最小值，平均值，和，行数
spark.sql("""
    select sum(price) as price 
    from temptable
    """).show()
# %%
spark.sql("""
    select 
    min(price) as minprice,
    max(price) as maxprice 
    from temptable
    """).show()
#%%
spark.sql("""
    select 
    count(*) from temptable
    """).show()
#%%
#分组group by
spark.sql("""
    select 
    model,count(*) as count 
    from temptable group by model
    """).show()
# %%
#having过滤,此处使用where会报错
spark.sql("""
    select model,count(*) as count 
    from temptable 
    group by model 
    having count(*) >= 50
    """).show()
#%%
#having与where的同时使用,
#having相对于分组而言，而where相对于行而言
#如果指定用group，大多数数据库是等价的
spark.sql("""
    select model,count(*) as count 
    from temptable 
    where trim(model) in ("A4","A5","A6") 
    group by model 
    having count(*) >= 50
    """).show()
#%%
spark.sql("select count(*) as count_all from temptable").show()
# %%
tempdf = spark.sql("""
    select transmission,
    count(*) as count 
    from temptable 
    where transmission in ('Automatic','Manual') 
    group by transmission 
    having count(*) >=50
    order by transmission,count
    """)
#%%
tempdf.show()
"""
    slect,from,where,group by,having,order by顺序
"""
print(type(tempdf))
# %%
mysql_OrderItems = spark.read.format("jdbc")\
    .option("driver","com.mysql.jdbc.Driver")\
    .option("url","jdbc:mysql://139.155.70.177:3306/Gadaite")\
    .option("dbtable","OrderItems")\
    .option("user","root")\
    .option("password","zzjz123")\
    .load()
# %%
mysql_OrderItems.createOrReplaceTempView("OrderItems")
spark.sql("select * from OrderItems").show()
#%%
#查询每个订单号，分组，并统计，排序
spark.sql("""
    select order_num,
    count(*) as order_lines 
    from OrderItems 
    group by order_num 
    order by order_lines
    """).show()
#%%
mysql_Products = spark.read.format("jdbc")\
    .option("driver","com.mysql.jdbc.Driver")\
    .option("url","jdbc:mysql://139.155.70.177:3306/Gadaite")\
    .option("dbtable","Products")\
    .option("user","root")\
    .option("password","zzjz123")\
    .load()
mysql_Products.createOrReplaceTempView("Products")
# %%
spark.sql("""
    select * from Products
    """).show()
#%%
spark.sql("""
    select 
    vend_id,
    min(prod_price) as cheapest_item 
    from Products 
    group by vend_id 
    order by cheapest_item
    """).show()
 
# %%
#查询所有订单号，分组，排序
spark.sql("""
    select 
    order_num  
    from OrderItems
    group by order_num 
    having sum(quantity) >=100 
    order by order_num
    """).show()
#%%
spark.sql("select * from OrderItems").show()
#%%
#确定最佳顾客，通过查询总花费
spark.sql("""
    select 
    order_num 
    from OrderItems 
    group by order_num 
    having sum(quantity*item_price) >=1000 
    order by order_num
    """).show()

#%%
spark.stop()
#%%
