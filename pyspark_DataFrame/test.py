from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.Driver.host","192.168.1.10")\
    .config("spark.ui.showConsoleProgress","false")\
    .appName("df").master("local[*]").getOrCreate()
#%%
from pyspark.ml.linalg import Vectors
from pyspark.sql.functions import col
data = [(1,2,),
        (3,4,),
        (5,6,)]
# df = spark.createDataFrame(data, ["id", "features"])
df = spark.createDataFrame(data, ["colname"])
df.show()