from pyhdfs import HdfsClient
client = HdfsClient(hosts='192.168.1.10,9000',user_name='root')

# 查看原始目录
print(client.listdir('/HadoopFileS'))

# 创建一个新的目录并查看
client.mkdirs('/HadoopFileS/newdir')
print(client.listdir('/HadoopFileS'))

print(client.exists('/HadoopFileS/newdir'))#判断文件或者目录是否存在

# 删除一个已有目录并查看
client.delete('/HadoopFileS/newdir')
print(client.listdir('/HadoopFileS'))
print(client.exists('/HadoopFileS/newdir'))#判断文件或者目录是否存在