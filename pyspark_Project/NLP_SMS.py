#%%
from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.driver.host","192.168.1.4")\
    .config("spark.ui.showConsoleProgress","false")\
    .appName("SMSS").master("local[*]").getOrCreate()
#%%
traindata = spark.read.csv("/mnt/e/win_ubuntu/Code/DataSet/MLdataset/SMSSpamCollectionTrain.csv",header=True,inferSchema=True)
testdata = spark.read.csv("/mnt/e/win_ubuntu/Code/DataSet/MLdataset/SMSSpamCollectionTest.csv",header=True,inferSchema=True)
traindata.show(2)
#%%
testdata.show(2)
#%%
from pyspark.sql.functions import length
traindata = traindata.withColumn("length",length(traindata["'text'"]))\
    .withColumnRenamed("'text'","text").withColumnRenamed("'type'","type").select("text","type","length")
#%%
traindata.show(10)
#%%
traindata.groupBy("type").mean().show()
#%%
from pyspark.ml.feature import CountVectorizer,Tokenizer,StopWordsRemover,IDF,StringIndexer
tokenizer = Tokenizer(inputCol = 'text', outputCol = 'token_text')
stop_remove = StopWordsRemover(inputCol = 'token_text', outputCol = 'stop_token')
count_vec = CountVectorizer(inputCol = 'stop_token', outputCol = 'c_vec')
idf = IDF(inputCol = 'c_vec', outputCol = 'tf_idf')
ham_spam_to_numeric = StringIndexer(inputCol = 'type', outputCol = 'label')
#%%
from pyspark.ml.feature import VectorAssembler
clean_up = VectorAssembler(inputCols = ['tf_idf', 'length'], outputCol = 'features')
#%%
from pyspark.ml.classification import NaiveBayes
nb = NaiveBayes()
#%%
from pyspark.ml import Pipeline
pipeline = Pipeline(stages=[ham_spam_to_numeric, tokenizer, stop_remove, count_vec, idf, clean_up])
#%%
cleaner = pipeline.fit(traindata)
clean_df = cleaner.transform(traindata)
clean_df = clean_df.select('label', 'features')
clean_df.show(3)
#%%
spam_detector = nb.fit(traindata)
predictions = spam_detector.transform(traindata)
predictions.show(3)