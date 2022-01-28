#%%
from pandas.core.dtypes.common import is_numeric_v_string_like
from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import NullType
spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
sc = spark.sparkContext

# %%
list=[(1,2,"",4,5)]
rdd_list = sc.parallelize(list)
df = spark.createDataFrame(rdd_list,schema=["imsi","col1","col2","col3","col4"])
#%%
df.show()
df.createOrReplaceTempView()
#%%
for row in df.collect():
    object_id = row[0]
    gjdq = row[1]
    company = row[2]
    username = row[3]
    importancelevel = row[4]
print(object_id,gjdq,company,username,importancelevel)
#%%
#排除缺失值，并配对，之后生成筛选sql语句
ret = []
for row in df.collect():
    object_id = str(row[0])
    if(len(object_id)!=0):
            ret.append(("object_id",object_id))
    gjdq = str(row[1])
    if(len(gjdq)!=0):
            ret.append(("gjdq",gjdq))
    company = str(row[2])
    print(type(company))
    if(len(company)!=0):
            ret.append(("company",company))
    username = str(row[3])
    if(len(username)!=0):
            ret.append(("username",username))
    importancelevel = str(row[4])
    if(len(importancelevel)!=0):
            ret.append(("importancelevel",importancelevel))
print(object_id,gjdq,company,username,importancelevel)
print(ret)
#%%
rdd_temp = spark.createDataFrame(ret)
rdd_temp.show()
#%%
print(ret[0])
#%%
str_original="select * from indf where "
for i in range(0,len(ret)):
    str_original = str_original + str(ret[i][0]) + "=%s"%(ret[i][1])+" and "
#print(str_original)
str_final = str_original[0:-4]
print(str_final)

