from pyspark.ml.linalg import Vectors

print(Vectors.dense([1, 2, 3, 4, 5, 6]))
print(Vectors.sparse(4, {1: 1.0, 3: 5.5}))
print(Vectors.sparse(4, [(1, 1.0), (3, 5.5)]))
print(Vectors.sparse(4, [1, 3], [1.0, 5.5]))