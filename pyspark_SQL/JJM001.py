#%%
from pyspark import SparkConf
from pyspark.sql import SparkSession
spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
sc = spark.sparkContext
#%%
list=[("高新区",16,4),("高新区",20,3),("天府新区",25,5),("双流区",30,4)]
rdd = sc.parallelize(list)
df = spark.createDataFrame(rdd,schema=["行政区","年龄","参保人数"])
df.show()
#%%
#注册临时表
df.createOrReplaceTempView("jiangjm")
#%%
tb1 = spark.sql("""select * from jiangjm""")
tb1.show()
#%%
spark.sql("""
    select * from jiangjm 
    """).show()
#%%
table1 = spark.sql("""
    select `行政区`,x1,x2,x3,x4,(t.x1+t.x2+t.x3+t.x4)as tt from (
    select 
    `行政区`,
    max(case when `年龄`=16 then `年龄` else 0 end) as x1,
    max(case when `年龄`=20 then `年龄` else 0 end) as x2,
    max(case when `年龄`=25 then `年龄` else 0 end) as x3,
    max(case when `年龄`=30 then `年龄` else 0 end) as x4,
    sum(`参保人数`)                                                                 
    from jiangjm    
    group by `行政区`                     
    )t
    """)
table1.createOrReplaceTempView("table1")
#%%
spark.sql("select * from table1").show()

#%%
#         importance：------  用数字作为表的字段会报错 ----------
spark.sql("""
    select `行政区`,`x1` as nihao
    -----,'x2' as 20s,'x3' as 25s,'x4' as 30s
    from table1
    """).show()

#%%
import pandas as pd
pddf = tb1.toPandas()
print(type(pddf))
#%%
def jjm():
    return 
pd.pivot_table(pddf, index=['行政区'], columns=['年龄'], values=['参保人数'], aggfunc='sum',fill_value=0)
#%%
