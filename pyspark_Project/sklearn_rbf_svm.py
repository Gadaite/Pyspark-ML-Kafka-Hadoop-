# %%
import os
import re
import random
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

import sklearn
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.neighbors import KNeighborsClassifier
from sklearn import svm
from sklearn import metrics
from sklearn.tree import DecisionTreeClassifier

# %%
# Read Data
s_file_path = r"/root/Github_files/spark_object/python_All/Dataset/线性不可分类别.csv"
df_in = pd.read_csv(s_file_path)

train_x = np.array([df_in["x"], df_in["y"]]).T
train_y = df_in["label"]
# %%
# 线性核函和RBF核SVM的指标
svm_linear = svm.SVC(kernel='linear', max_iter=100, C=1.0, gamma=0.5)
svm_linear.fit(train_x, train_y)
pred_y_linear = svm_linear.predict(train_x)
print('Linear SVM, acc: %.6f' % metrics.accuracy_score(train_y, pred_y_linear))
print('Linear SVM, p: : %.6f' % metrics.precision_score(train_y, pred_y_linear))
print('Linear SVM, r: : %.6f' % metrics.recall_score(train_y, pred_y_linear))
print('Linear SVM, f1: : %.6f' % metrics.f1_score(train_y, pred_y_linear))
print()

svm_rbf = svm.SVC(kernel='rbf', max_iter=100, C=1.0, gamma=0.5)
svm_rbf.fit(train_x, train_y)
pred_y_rbf = svm_rbf.predict(train_x)

print('RBF SVM, acc: : %.6f' % metrics.accuracy_score(train_y, pred_y_rbf))
print('RBF SVM, acc: : %.6f' % metrics.precision_score(train_y, pred_y_rbf))
print('RBF SVM, acc: : %.6f' % metrics.recall_score(train_y, pred_y_rbf))
print('RBF SVM, acc: : %.6f' % metrics.f1_score(train_y, pred_y_rbf))
print()
# %%
# 线性核函和RBF核SVM的图示效果
x_pred_linear_1, y_pred_linear_1 = [], []
x_pred_linear_2, y_pred_linear_2 = [], []

for i, e in enumerate(pred_y_linear):
    if e == 0:
        x_pred_linear_1.append(train_x[i, 0])
        y_pred_linear_1.append(train_x[i, 1])
    else:
        x_pred_linear_2.append(train_x[i, 0])
        y_pred_linear_2.append(train_x[i, 1])

x_pred_rbf_1, y_pred_rbf_1 = [], []
x_pred_rbf_2, y_pred_rbf_2 = [], []

for i, e in enumerate(pred_y_rbf):
    if e == 0:
        x_pred_rbf_1.append(train_x[i, 0])
        y_pred_rbf_1.append(train_x[i, 1])
    else:
        x_pred_rbf_2.append(train_x[i, 0])
        y_pred_rbf_2.append(train_x[i, 1])
plt.figure(figsize=(32, 16), dpi=80)
plt.subplot(121)
# %%
plt.plot(x_pred_linear_1, y_pred_linear_1, 'mo')
# %%
plt.plot(x_pred_linear_2, y_pred_linear_2, 'co')
# %%
plt.xlabel('x')
plt.ylabel('y')
plt.grid(True)
plt.title("sklearn svm case:svm linear kernel")
plt.subplot(122)
# %%
plt.plot(x_pred_rbf_1, y_pred_rbf_1, 'yo')
# %%
plt.plot(x_pred_rbf_2, y_pred_rbf_2, 'ko')
# %%
plt.xlabel('x')
plt.ylabel('y')
# %%
plt.grid(True)
plt.title("sklearn svm case:svm rbf kernel")
# %%
plt.savefig("tmp.jpg")
# %%