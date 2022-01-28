#%%
from pyspark import SparkConf,SparkContext
from pyspark import sql
from pyspark.sql import SparkSession
spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
sc = spark.sparkContext
 
# %%
rdd1 = sc.parallelize([(22,'xiaobao'),(23,'chuyu'),(23,'caffe')])
df1 = spark.createDataFrame(rdd1,['age','name'])
# %%
df1.createOrReplaceTempView("mytable")
df1.show()
# %%
spark.sql("select * from mytable where name='xiaobao'").show()
# %%
jdbcDF = spark.read.format("jdbc")\
    .option("driver","com.mysql.jdbc.Driver")\
    .option("url","jdbc:mysql://139.155.70.177:3306/Gadaite")\
    .option("dbtable","audi")\
    .option("user","root")\
    .option("password","zzjz123")\
    .load()
# %%
jdbcDF.createOrReplaceTempView("temptable")
spark.sql("select * from temptable").show(10)
# %%
#所有奥迪A1的数量
spark.sql("select * from temptable").show()
# %%
spark.sql("select * from temptable where engineSize = 2.0").show()
#%%
spark.sql("select * from temptable where year=2018").show()
# %%
spark.sql("select * from temptable where model='A4'").show()
# %%
spark.sql("select count(*) from temptable where trim(model)='A4'").show()
 
# %%
spark.sql("select * from temptable where trim(model)='A4'").show()
 
# %%
spark.sql("""
    select * 
    from temptable 
    where trim(model)='A4' 
    order by 
    price limit 15
    """).show()
# %%
 
#判断是否区分大小写
#结果区分大小写
spark.sql("""
    select * from temptable 
    where trim(transmission)='Manual' 
    limit 15
    """).show()
spark.sql("""
    select * from temptable 
    where trim(transmission)='manual' 
    limit 15
    """).show()
# %%
#-------%%%通配符
spark.sql("""
    select * from temptable 
    where fuelType like 'Pet%'
    """).show()
# %%
spark.sql("""
    select * from temptable 
    where mpg like '%1.4%'
    """).show()
# %%
spark.sql("""
    select * from temptable 
    where not engineSize like '%2.0%'
    """).show()
# %%
spark.sql("""
    select * from temptable 
    where ((engineSize like '%2.0%') 
    and (fuelType = 'Petrol'))
    """).show()
# %%
#组合计算
#此处明显验证出model前面有空格
spark.sql("""
    select Concat(transmission,model) 
    from temptable 
    order by price desc 
    limit 10
    """).show()
# %%
spark.sql("""
    select Concat(transmission,ltrim(model)) 
    from temptable 
    order by price desc 
    limit 10
    """).show()
#%%
spark.sql("""
    select Concat(transmission,'------',ltrim(model)) 
    from temptable 
    order by price desc 
    limit 10
    """).show()
# %%
spark.sql("""
    select Concat(transmission,'------',ltrim(model)) 
    as car_info 
    from temptable 
    order by price desc 
    limit 10
    """).show()
# %%
spark.sql("""
    select model,year,price,mileage,engineSize, 
    price / engineSize as per_engine_price 
    from temptable 
    order by price desc 
    limit 10
    """).show()
# %%
#测试计算
spark.sql("select 3*2,trim(' riven  ')").show()
# %%
"""
    以下不可用，会报错
    spark.sql("select Curdate()")
"""
# %%
spark.sql("select * from temptable").show()
# %%
#转换成大写字符串
spark.sql("""select  
    transmission ,fuelType,
    upper(transmission) as TRANSMISSION,
    UPPER(fuelType) AS FUELTYPE  
    from temptable 
    LIMIT 15 
    """).show()
# %%
#转换成小写字符串
spark.sql("""select  
    transmission ,fuelType,
    lower(transmission) as transmission,
    lower(fuelType) AS fueltype  
    from temptable 
    LIMIT 15 
    """).show()
# %%
#返回字符串的长度
spark.sql("""select  
    transmission ,fuelType,
    length(transmission) as transmission_lens,
    length(fuelType) AS fuelType_lens  
    from temptable 
    LIMIT 15 
    """).show()
#%%
#返回字符串左起的的n个字符
spark.sql("""select  
    transmission ,fuelType,
    left(transmission,2) as transmission_two,
    left(fuelType,3) AS fueltype_3  
    from temptable 
    LIMIT 15 
    """).show()
# %%
#返回字符串右起的n个字符
spark.sql("""select  
    transmission ,fuelType,
    right(transmission,3) as transmission_two,
    right(fuelType,3) AS fueltype_3  
    from temptable 
    LIMIT 15 
    """).show()
#%%
#提取字符串的组成部分,类似于python的切片操作
#substr(a,b,c),y也可用substring
spark.sql("""select  
    transmission ,fuelType,
    substr(transmission,2,2) as transmission_res,
    substr(fuelType,-2,3) AS fueltype_res   
    from temptable 
    LIMIT 15 
    """).show()
spark.sql("""select  
    transmission ,fuelType,
    substring(transmission,2,2) as transmission_res,
    substring(fuelType,-2,3) AS fueltype_res   
    from temptable 
    LIMIT 15 
    """).show()
# %%
# #返回字符串长度的其它函数写法，pyspark不支持
# spark.sql("""select  
#     transmission ,fuelType,
#     len(transmission) as transmission_lens,
#     len(fuelType) AS fuelType_lens  
#     from temptable 
#     LIMIT 15 
#     """).show()
# %%
#soundex描述其语音表示字母数字模式的算法
spark.sql("""select  
    fuelType 
    from temptable 
    where soundex(fuelType) = soundex("Petol") 
    LIMIT 15 
    """).show()
# %%
spark.sql("""
    select * from temptable limit 10
    """).show()
#%%
from pyspark.sql.functions import *
#%%
#日期和时间处理函数
spark.sql("""
    select * from temptable limit 5
    """).show()
# %%
#换个带时间的表
jdbcdf = spark.read.format("jdbc")\
    .option("driver","com.mysql.jdbc.Driver")\
    .option("url","jdbc:mysql://139.155.70.177:3306/Gadaite")\
    .option("dbtable","footbshootouts")\
    .option("user","root")\
    .option("password","zzjz123")\
    .load()
# %%
jdbcdf.createOrReplaceTempView("mytable")
# %%
spark.sql("""select 
    * from mytable limit 20   
    """).show()
# %%
#字段需要时date，不是varchar
# spark.sql("""select 
#     winner from mytable 
#     where datepart(yy,date)=1973
#     """).show()
 
# %%
spark.sql("""
    select winner,away_team,date,home_team,
    upper(left(away_team,2) || left(winner,3)) 
    as winer_info from mytable 
    limit 15
    """).show()
# %%
spark.sql("""
    select winner,away_team,
    left(away_team,2) || left(winner,3) as winer_info 
    from mytable 
    limit 15
    """).show()
# %%
spark.sql("""
    select winner,away_team,
    left(away_team,2) as winer_info 
    from mytable 
    limit 15
    """).show()
# %%
spark.sql("""
    select winner,away_team,
    left(winner,3) as winer_info 
    from mytable 
    limit 15
    """).show()
# %%
spark.sql("""
    select winner,away_team,
    (away_team || winner) as winer_info 
    from mytable 
    limit 15
    """).show()
# %%
spark.sql("""
    select winner,away_team,
    (away_team || winner) as winer_info 
    from mytable 
    limit 15
    """).show()
# %%
spark.sql("select count(*) as count from mytable").show()
# %%
spark.sql("""
    select winner,away_team,
    (away_team || winner) as winer_info 
    from mytable 
    limit 15
    """).show()
# %%
spark.sql("""
    select winner,away_team,date,home_team,
    upper(left(away_team,2) || "---" || left(winner,3)) 
    as winer_info from mytable 
    limit 15
    """).show()
# %%
spark.stop()
#%%
