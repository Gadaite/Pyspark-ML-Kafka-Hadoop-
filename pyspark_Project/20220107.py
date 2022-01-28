#%%
from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.util import MLUtils
from pyspark.ml.classification import LogisticRegression
from pyspark.sql import SparkSession

spark = SparkSession.builder.config("spark.driver.host","192.168.1.4")\
    .config("spark.ui.showConsoleProgress","false")\
    .appName("app").master("local[*]").getOrCreate()
#%%
training = spark\
        .read\
        .format("libsvm")\
        .load("/mnt/e/win_ubuntu/Code/DataSet/ZOthers/sample_multiclass_classification_data.txt")
lr = LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
lrModel = lr.fit(training)
#%%
# 打印多项式逻辑回归的系数和截距
lrModel.coefficientMatrix
#%%
lrModel.interceptVector
#%%
# 获得每次迭代的目标列表
trainingSummary = lrModel.summary
objectiveHistory = trainingSummary.objectiveHistory
objectiveHistory
#%%
#%%
def foreach(function, iterator):
    for item in iterator:
        function(item)
#%%
# 对于多类，我们可以基于每个标签检查度量
# 标签假阳性率
trainingSummary.falsePositiveRateByLabel
#%% 
# 标签真阳性率
trainingSummary.truePositiveRateByLabel
#%%
# 标签精度
trainingSummary.precisionByLabel
# %%
# 标签召回
trainingSummary.recallByLabel
#%%
# 标签F-measure
trainingSummary.fMeasureByLabel
#%%
spark.stop()
