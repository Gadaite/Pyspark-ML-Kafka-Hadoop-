#%%
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
spark = SparkSession.builder.config("spark.driver.host","192.168.1.4")\
    .config("spark.ui.showConsoleProgress","false")\
    .appName("StringIndexerT").master("local[*]").getOrCreate()
#%%
from pyspark.sql.types import Row
data = spark.createDataFrame([
    Row(1,2),
    Row(11,12),
    Row(15,16),
    Row(19,20)
],["name","version"])
data.show()
#%%
vectorAssembler = VectorAssembler(inputCols=['name', 'version'],outputCol="res")
res = vectorAssembler.transform(data)
res.show()
#%%
res.printSchema()
spark.stop()
#%%
