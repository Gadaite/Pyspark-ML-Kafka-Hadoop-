from pyspark.ml.linalg import Vectors
v1 = Vectors.dense([1.0,2.0])
print(type(v1))
print(v1)
v2 = v1.toArray()
print(type(v2))
v3 = v2.tolist()
print(type(v3))
import numpy as np
x= np.array([5.0,6.0,7.0])
y = Vectors.dense(x)
print(y)
print(type(y))

