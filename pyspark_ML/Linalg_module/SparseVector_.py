from pyspark.ml.linalg import SparseVector
a= SparseVector(4, [1, 3], [3.0, 4.0])
print(a.dot(a))

import array
a.dot(array.array('d', [1., 2., 3., 4.]))
b = SparseVector(4, [2], [1.0])
print(a.dot(b))

import numpy as np
print(a.dot(np.array([[1, 1], [2, 2], [3, 3], [4, 4]])))

print(a.indices)

a = SparseVector(4, [0, 1], [3., -4.])
print(a.norm(1), a.norm(2))

print(a.size)

a = SparseVector(4, [1, 3], [3.0, 4.0])
print(a.squared_distance(a))
print(a.squared_distance(array.array('d', [1., 2., 3., 4.])))
print(a.squared_distance(np.array([1., 2., 3., 4.])))
b = SparseVector(4, [2], [1.0])
print(a.squared_distance(b))

print(a.toArray())
print(type(a.toArray()))