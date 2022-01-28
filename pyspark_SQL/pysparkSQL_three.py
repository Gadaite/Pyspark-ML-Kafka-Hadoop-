#%%
#导入相关库类
from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
 
# %%
#定义mysql数据库连接方法(参数为库名，表名)，并创建dataframe临时表
def connect_DB(db_name,tb_name):
    #连接数据库
    jdbcDF = spark.read.format("jdbc")\
    .option("driver","com.mysql.jdbc.Driver")\
    .option("url","jdbc:mysql://139.155.70.177:3306/%s"%(db_name))\
    .option("dbtable","%s"%(tb_name))\
    .option("user","root")\
    .option("password","zzjz123")\
    .load()
    #创建临时表
    jdbcDF.createOrReplaceTempView(tb_name)
#%%
connect_DB("Gadaite","Orders")
# %%
#查看Orders表信息
spark.sql("""
    select * from Orders
    """).show()
#%%
connect_DB("Gadaite","OrderItems")
# %%
#查看OrderItems表信息
spark.sql("""
    select * from OrderItems
    """).show()
# %%
#%%
connect_DB("Gadaite","Customers")
spark.sql("""
    select * from Customers
    """).show()
# %%
"""
    Orders:订单编号，客户ID，日期
    OrderItems:订单物品存储
    Customers:顾客实际信息
    Q：列出订购物品RGAN01的所有顾客ID
"""
#%%
#非子查询
spark.sql("""
    select order_num 
    from OrderItems 
    where prod_id="RGAN01"
    """).show()
"结果：20007，20008"
spark.sql("""
    select cust_id 
    from Orders 
    where order_num in (20007,20008)
    """).show()
"结果：1000000004，1000000004"
 
#%%
#使用子查询,从内向外处理
spark.sql("""
    select cust_id 
    from Orders 
    where order_num in (
        select order_num 
        from OrderItems 
        where prod_id='RGAN01'
    )
    """).show()
 
# %%
#可在基础上再检索顾客的所有信息，或者部分信息
#以子查询为例,过多嵌套会降低性能
spark.sql("""
    select * 
    from Customers 
    where cust_id in (
        select cust_id 
        from Orders 
        where order_num in (
            select order_num 
            from OrderItems 
            where prod_id='RGAN01'
        )
    )
    """).show()
#%%
#作为计算字段使用子查询
#显示Customers表中每个顾客的订单总数
#%%
#以下执行可能会报错,且是一种错误的写法
#如果报错按下面格式书写，使用完全限定列名，其原因关于DBMS无法判断：
spark.sql("""
    select cust_name,cust_state,
    (select count(*)
        from Orders
    ) as count 
    from Customers 
    order by cust_name
    """).show()
# %%
#正确的写法
spark.sql("""
    select cust_name,cust_state,
    (select count(*)
        from Orders 
        where Customers.cust_id=Orders.cust_id
    ) as count 
    from Customers 
    order by cust_name
    """).show()
#理解：
"""
    该查询会新增新的一个字段，必定是作为计算字段的查询
    前面例子是作为筛选条件查询，不会生成一个新的字段
    对应位置即可
    思路：
    在外层按照行来看，每行会取到一个对应的cust_id,
    按照这个cust_id对另一个表先进行条件筛选，再count的行数就自然是个数了
"""
# %%
#Q：返回价格>=10的产品(不是总价)的顾客列表，先使用OrderItems
spark.sql("""
    select cust_id 
    from Orders  
    where order_num in (
        select order_num 
        from OrderItems 
        where item_price >=10
    )
    """).show()
#%%
#Q：确定哪些订单购买了prod_id=BR01产品
#   返回每个产品的顾客id和订单日期，按订单日期排序
spark.sql("""
    select cust_id,order_date 
    from Orders 
    where order_num in (
        select order_num 
        from OrderItems 
        where prod_id='BR01'
    ) 
    order by order_date
    """).show()
 
#%%
#Q：在上一个Q上，返回prod_id为BR01的产品的所有顾客的电子邮件
spark.sql("""
    select cust_email 
    from Customers 
    where cust_id in (
        select cust_id 
        from Orders 
        where order_num in (
            select order_num 
            from OrderItems 
            where prod_id='BR01' 
        ----order by order_date
        ) 
            order by order_date 
    ) 
    """).show()
 
#%%
#需要一个顾客列表，包含已订购的总金额，返回顾客ID和总数(从大到小)
spark.sql("""
    select cust_id,
    (select sum(quantity*item_price) 
        from OrderItems 
        where OrderItems.order_num = Orders.order_num
    )as total_ordered 
    from Orders 
    order by total_ordered desc
    """).show()
 
#%%
#注册临时表Products
connect_DB("Gadaite","Products")
#%%
#从Products表中检索prod_name，计算列quant_sold
#包含所售产品总数
#在Customers上使用sum(quantity)检索
spark.sql("""
    select prod_name,
    (
        select 
        sum(quantity) 
        from OrderItems 
        where OrderItems.prod_id=Products.prod_id 
    ) as quant_sold 
    from Products
    """).show()
#%%
spark.stop()
#%%
