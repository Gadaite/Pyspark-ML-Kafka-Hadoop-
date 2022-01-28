
#%%
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
 
from pyspark.sql.types import StructType, StructField, LongType, StringType
 
# 创建SparkSession连接到Spark集群-SparkSession.builder.appName('name').getOrCreate()
spark = SparkSession \
    .builder \
    .appName('create_df_QUENTINNE') \
    .getOrCreate()
 
 
# 创建DataFrame,可以从不同的数据创建，以下进行对个数据源读取创建说明
# 方法1：从RDD创建新的DataFrame
def create_df_from_rdd():
    
    spark_rdd = spark.sparkContext.parallelize([
        (123, "Katie", 19, "brown"),
        (456, "Michael", 22, "green"),
        (789, "Simone", 23, "blue")])
 
    # 设置dataFrame将要使用的数据模型，定义列名，类型和是否为能为空
    schema = StructType([StructField("id", LongType(), True),
                         StructField("name", StringType(), True),
                         StructField("age", LongType(), True),
                         StructField("eyeColor", StringType(), True)])
    # 创建DataFrame
    spark_df_from_rdd = spark.createDataFrame(spark_rdd, schema)
    # 注册为临时表
    # 打印dataframe
    spark_df_from_rdd.show()
    spark_df_from_rdd.registerTempTable("swimmers")
    # 使用Sql语句
    data = spark.sql("select * from swimmers")
    # 将数据转换List，这样就可以查看dataframe的数据元素的样式
    print(data.collect())
    # 以表格形式展示数据
    data.show()
 
    print("{}{}".format("swimmer numbers : ", spark_df_from_rdd.count()))
#%%
# 方法2：从csv文件创建新的DataFrame
def create_df_from_csv():
    spark_df_from_csv = spark.read.csv('AlgorithmCompare.csv', header=True, inferSchema=True)
    spark_df_from_csv.show()
 
# 方法3：从pandas_df创建新的DataFrame
def create_df_from_pandas():
    # 从Python pandas获取数据
    df = pd.DataFrame(np.random.random((4, 4)))
    spark_df_from_pandas = spark.createDataFrame(df, schema=['a', 'b', 'c', 'd'])
    spark_df_from_pandas.show()
 
 
if __name__ == '__main__':
    create_df_from_rdd()
    ##create_df_from_csv()
    ##create_df_from_pandas()
#%%