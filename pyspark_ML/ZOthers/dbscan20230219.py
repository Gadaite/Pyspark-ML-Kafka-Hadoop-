

#示例代码
import numpy as np
import matplotlib.pyplot as plt

#定义dbscan类
class DBSCAN(object):

    # 初始化
    def __init__(self, X, eps, min_samples):
        self.X = X
        self.eps = eps
        self.min_samples = min_samples
        self.cluster_num = 0
        self.labels = np.zeros(X.shape[0])


    #计算欧氏距离
    def euc_distance(self, x, y):
        return np.sqrt(np.sum((x-y)**2))


    #在eps范围内查找点
    def region_query(self, x):
        neighbors = []
        for i in range(self.X.shape[0]):
            if self.euc_distance(x, self.X[i]) < self.eps:
                neighbors.append(i)
        return neighbors


    # 对输入的数据集聚类
    def fit(self):
        visited = np.zeros(self.X.shape[0])
        for i in range(self.X.shape[0]):
            if visited[i] == 0:
                visited[i] = 1
                neighbor_points = self.region_query(self.X[i])
                if len(neighbor_points) < self.min_samples:
                    self.labels[i] = -1
                else:
                    self.cluster_num += 1
                    self.expand_cluster(i, neighbor_points, visited)
        return self.labels


    #聚类扩展
    def expand_cluster(self, point_index, neighbor_points, visited):
        self.labels[point_index] = self.cluster_num
        for i in neighbor_points:
            if visited[i] == 0:
                visited[i] = 1
                new_neighbor_points = self.region_query(self.X[i])
                if len(new_neighbor_points) >= self.min_samples:
                    neighbor_points += new_neighbor_points
            if self.labels[i] == 0:
                self.labels[i] = self.cluster_num


#准备数据
# X = np.array([[-2,-2],[-2,1.5],[-1,0],[0,1],[1,1],[2,1],[3,0],[3,-2],[2,-3],[0,-2],[-1,-2]])
cluster_1 = np.random.uniform(0.0,1.0,(100,2))
cluster_2 = np.random.uniform(10.0,11.0,(100,2))

X = cluster_2
#定义dbscan参数
eps = 1.5
min_samples = 3

#实例化dbscan
clf = DBSCAN(X, eps, min_samples)
y_pred = clf.fit()

#绘图
color = ['r', 'g', 'b', 'y', 'c', 'm']
marker = ['o', 's', '^', '*', 'p', 'X']

for i in range(X.shape[0]):
    plt.scatter(X[i, 0], X[i, 1], color=color[int(y_pred[i])], marker=marker[int(y_pred[i])])

plt.show()