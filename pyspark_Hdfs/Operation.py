from pyhdfs import HdfsClient
import os
print(os.listdir('/root'))

# 连接信息
client = HdfsClient(hosts="192.168.1.10,9000",user_name='root')

# 查看原始hdfs路径下的文件
print(client.listdir('/HadoopFileS/DataSet/titanic'))

# 上传文件到hdfs
# 没有该文件才进行上传，否则报错
if 'emp.txt' not in client.listdir('/HadoopFileS/DataSet/titanic'):
    client.copy_from_local("/root/emp.txt","/HadoopFileS/DataSet/titanic/emp.txt")
print(client.listdir('/HadoopFileS/DataSet/titanic'))

# 从hdfs下载文件
# 没有该文件才进行上传，否则报错
if 'test.csv' not in os.listdir('/root'):
    client.copy_to_local('/HadoopFileS/DataSet/titanic/test.csv','/root/test.csv')
print(os.listdir('/root'))

# # 删除hdfs文件
client.delete("/HadoopFileS/DataSet/titanic/emp.txt")
print(client.listdir('/HadoopFileS/DataSet/titanic'))

# 读取hdfs文件
print(client.open('/HadoopFileS/DataSet/titanic/test.csv'))#<urllib3.response.HTTPResponse object at 0x7fa54c987b00>
print(client.open('/HadoopFileS/DataSet/titanic/test.csv').read())
print(type(client.open('/HadoopFileS/DataSet/titanic/test.csv').read()))#<class 'bytes'>
print(str(client.open('/HadoopFileS/DataSet/titanic/test.csv').read(),encoding='utf8')) #转为字符串输出

# 对hdfs文件进行插入
client.append(path="/HadoopFileS/DataSet/titanic/test.csv",data="""1310,3,"Peter, Master. Michael J",male,,1,1,2668,22.3583,,C\n""".encode(),)

# 输入的数据必须是二进制编码
print(str(client.open('/HadoopFileS/DataSet/titanic/test.csv').read(),encoding='utf8')) #转为字符串输出

# 删除hdfs的数据，只能重新导入，同样hive使用sql的时候也是如此

