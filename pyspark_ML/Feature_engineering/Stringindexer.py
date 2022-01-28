#%%
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer
spark = SparkSession.builder.config("spark.driver.host","192.168.1.4")\
    .config("spark.ui.showConsoleProgress","false")\
    .appName("StringIndexerT").master("local[*]").getOrCreate()
#%%
from pyspark.sql.types import Row
data = spark.createDataFrame([
    Row("hadoop"),
    Row("spark"),
    Row("flink"),
    Row("kafka"),
    Row("java"),
    Row("flink"),
    Row("kafka"),
    Row("python")
],["name"])
data.show()
#%%
stringIndexer = StringIndexer(inputCol=data.columns[0],outputCol="ADD")
model = stringIndexer.fit(data)
outdata = model.transform(data)
outdata.show()
#%%
outdata.printSchema()
spark.stop()
#%%