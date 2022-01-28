#%%
from pyspark import SparkConf,SparkContext
from  pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StructField, StructType,DataType
spark = SparkSession.builder.appName("myapp").getOrCreate()
sc = spark.sparkContext
#%%
rdd_test = spark.sparkContext.parallelize([(1,2,3),(4,5,6),(7,8,9)])

print(type(rdd_test))
#%%
rdd_test.countByKey()
#%%
rdd_test.collect()
#%%
rdd_test.count()
#%%
rdd_test.first()
#%%
rdd = spark.sparkContext.parallelize([[1,2,3],[4,5,6],[7,8,9]])
rdd.collect()[0][0]
rdds =rdd.map(lambda x:[x[0]**2,x[1]**3,x[2]**4])
rdds.collect()
#%%
from pyspark.sql.types import StructType, StructField, LongType, StringType
schema_test = StructType([
    StructField("col1",IntegerType(),True),
    StructField("col2",IntegerType(),True),
    StructField("col3",IntegerType(),True)

])
#%%
indf_one = spark.createDataFrame(rdd_test,schema_test)
indf_two = spark.createDataFrame(rdds,schema_test)
#%%
indf_one.printSchema()
indf_two.printSchema()
#%%
indf_one.show()
indf_two.show()
#%%
import math
#%%
list_s = []
for row in indf_two.collect():
    print(row)
    list_s.append([row["col1"]+1,row["col2"]+1,row["col3"]+1])
    #print(list_row)
#%%
list_s
#%%
rdd_outdf = sc.parallelize(list_s)
rdd_outdf.collect()
#%%
outdf_schema = StructType([
    StructField("square+1",IntegerType()),
    StructField("cube+1",IntegerType()),
    StructField("quartic+1",IntegerType())
])
#%%
outdf = spark.createDataFrame(rdd_outdf,outdf_schema)
outdf.show()
#%%
outdf.createOrReplaceTempView("view")
#%%
spark.sql("""
    select `square+1` as column_one,
    `cube+1` as column_two,
    `quartic+1` as  column_three 
    from view 
    limit 2
    """).show()