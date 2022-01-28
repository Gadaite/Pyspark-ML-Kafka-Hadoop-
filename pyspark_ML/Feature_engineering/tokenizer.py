#%%
from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.driver.host","192.168.1.4")\
    .config("spark.ui.showConsoleProgress","false")\
    .appName("Tokenizer").master("local[*]").getOrCreate()
#%%
import this
#%%
data = spark.createDataFrame([
    ("Beautiful is better than ugly",),
    ("Explicit is better than implicit",),
    ("Simple is better than complex",),
    ("Complex is better than complicated",),
    ("Flat is better than nested",),
    ("Sparse is better than dense",)
],["python_This"])
data.show()
#%%
from pyspark.ml.feature import Tokenizer
tokenizer = Tokenizer(inputCol="python_This",outputCol="res")
data = tokenizer.transform(data)
data.show()
# %%
data.head(1)
# %%
data.printSchema()
