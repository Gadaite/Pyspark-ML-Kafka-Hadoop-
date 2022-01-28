from pyspark.ml.linalg import DenseMatrix
m = DenseMatrix(2, 2, range(4))
print(m)
print(m.toArray())
print(m.toSparse())
print(m.toSparse().toArray())
print(type(m.toSparse().toArray()))