#%%
from pyspark import SparkConf, SparkContext

def f(x):
    print(x)

def parseInput(row):
    import re
    pattern = re.compile(r'\(\'(a-z)\', ([0-9])\)')
    row_split = pattern.split(row)
    return (row_split[1], int(row_split[2]))


def extractInformation(row):
    import re
    import numpy as np
    selected_indices = [
        2, 4, 5, 6, 7, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18
    ]
    record_split = re \
        .compile(
        r'([\s]{19})([0-9]{1})([\s]{40})'
    )
    try:
        re = np.array(record_split.split(row))[selected_indices]
    except:
        re = np.array(['-99'] * len(selected_indices))
    return re



# Initialize the sparkcontext
conf = SparkConf().setMaster("local[*]").setAppName("Test2_2")

sc = SparkContext(conf = conf)
#%%
# create the rdd
data = sc.parallelize(
    [('Amber', 22), ('Alfred', 23), ('Skye', 4), ('Albert', 12), ('Amber', 9)]
)

data_heterogenous = sc.parallelize([
    ('Ferrari', 'fase'),
    {'Porsche', 100000},
    ['Spain', 'visited', 4504]
]).collect()

print(list(data_heterogenous[1])[0])
#%%
# read the rdd
data_from_file = sc.textFile('/root/Github_files/python_All/Dataset/T1.txt', 4)

print(data_from_file.take(1))
#%%

data_from_file_conv = data_from_file.map(extractInformation)

print(data_from_file_conv.take(1))

# map
data_map = data_from_file_conv.map(lambda row: int(row[16]))

print(data_map.take(5))

# filter
data_filtered = data_from_file_conv.filter(lambda row: row[16] == '2014' and row[21] == '0')

print(data_filtered.count())

# flatMap
data_flatMap = data_from_file_conv.flatMap(lambda row: (row[16], row[16] + 1))

print(data_flatMap)

# distinct
data_distinct = data_from_file_conv.map(lambda row: row[5]).distinct()

print(data_distinct.collect())

# sample
faction = 0.1
data_sample = data_from_file_conv.sample(False, faction, 666)

print('Original dataset: {0}, sample: {1}' \
        .format(data_from_file_conv.count(), data_sample.count()))

# leftOuterJoin
rdd1 = sc.parallelize([('a', 1), ('b', 4), ('c', 10)])
rdd2 = sc.parallelize([('a', 4), ('a', 1), ('b', 6), ('d', 15)])
rdd3 = rdd1.leftOuterJoin(rdd2)

rdd4 = rdd1.join(rdd2)
print(rdd4.collect())

rdd5 = rdd1.intersection(rdd2)
print(rdd5.collect())

# repartition
rdd1 = rdd1.repartition(4)
print(len(rdd1.glom().collect()))

# take
data_take = data_from_file_conv.take(1)

data_take_sample = data_from_file_conv.take(False, 1, 667)

# collect

# reduce \ reduceByKey
data_reduce = rdd1.map(lambda row: row[1]).reduce(lambda x, y: x + y)

data_key = sc.parallelize([('a', 4), ('b', 3), ('c', 2), ('a', 8), ('d', 2), ('b', 1), ('d', 3)], 4)

data_reduceByKey = data_key.reduceByKey(lambda x, y: x + y).collect()

# count
##data_from_file_conv_.count()

data_key.countByKey().item()

# saveAsTextFile
data_key.saveAsTextFile('e://data_key.txt')

data_key_reread = sc.textFile('e://data_key.txt').map(parseInput).collect()

# foreach
data_key.foreach(f)


