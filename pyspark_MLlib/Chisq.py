#%%
from pyspark.sql import SparkSession 
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.linalg import Matrix
from pyspark.mllib.stat import Statistics
from pyspark.mllib import linalg
spark = SparkSession.builder.config("spark.Driver.host","192.168.1.4").config("spark.ui.showConsoleProgress","false")\
    .appName("app").master("local[*]").getOrCreate()
#%%
vd = Vectors.dense(1,2,3,4,5)
vdres = Statistics.chiSqTest(vd)
print(vdres)
#%%
mtx = linalg.Matrices.dense(3,2,[1,3,5,2,4,6])
mtxres = Statistics.chiSqTest(mtx)
print(mtxres)
#%%
spark.stop()