# class pyspark.ml.linalg.SparseMatrix(numRows, numCols, colPtrs, rowIndices, values, isTransposed=False)[source]
# Sparse Matrix stored in CSC format.
#
# toArray()[source]
# Return a numpy.ndarray
#
# toDense()[source]
# class pyspark.ml.linalg.Matrices[source]
# static dense(numRows, numCols, values)[source]
# Create a DenseMatrix
#
# static sparse(numRows, numCols, colPtrs, rowIndices, values)[source]
# Create a SparseMatrix