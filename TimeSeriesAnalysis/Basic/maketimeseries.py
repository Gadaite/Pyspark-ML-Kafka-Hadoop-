import pandas as pd
import numpy as np
import datetime as dt
from pyspark.sql import SparkSession
spark = SparkSession.builder.config('spark.driver.host','192.168.1.10').\
    appName("basic").master("local[*]").enableHiveSupport().getOrCreate()
# 创建时间序列
rng = pd.date_range('2022/01/04',periods=10,freq='D')
print(rng)
time=pd.Series(np.random.randn(20),
           index=pd.date_range(dt.datetime(2016,1,1),periods=20))
print(time)
# 剔除指定条件的数据
time.truncate(after='2016-1-10')
# 偏移量 ： TIME OFFSETS
pd.Timedelta('1 day')