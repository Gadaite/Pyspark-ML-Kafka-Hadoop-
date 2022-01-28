from pyspark.ml.linalg import Vectors
v = Vectors.dense([1.0, 2.0])
u = Vectors.dense([3.0, 4.0])
print(u + v)
print(2 - u)
print(u/2)
print(u * v)
print(u%2)
print(-u)

import array
from pyspark.ml.linalg import DenseVector,SparseVector
dense = DenseVector(array.array('d', [1., 2.]))
print(dense.dot(dense))

print(dense.dot(SparseVector(2, [0, 1], [2., 1.])))
print(dense.dot(range(1, 3)))

import numpy as np
print(dense.dot(np.array(range(2, 4))))

a = DenseVector([0, -1, 2, -3])
print(a.norm(2),a.norm(1))
print(a.numNonzeros())

dense1 = DenseVector(array.array('d', [1., 2.]))
print(dense1.squared_distance(dense1))
dense2 = np.array([2., 1.])
print(dense1.squared_distance(dense2))
dense3 = [2., 1.]
print(dense1.squared_distance(dense3))
sparse1 = SparseVector(2, [0, 1], [2., 1.])
print(dense1.squared_distance(sparse1))
sparse1 = SparseVector(2, [0, 1], [2., 1.])
print(dense1.squared_distance(sparse1))

print(sparse1.toArray())
print(type(sparse1.toArray()))

print(type(array.array('i', [1, 2])))