from pyhdfs import HdfsClient
client = HdfsClient(hosts="192.168.1.10,9000",user_name="root")

# 返回这个用户的根目录
print(client.get_home_directory())

# 返回可用的namenode节点
print(client.get_active_namenode())

# 返回指定目录下的所有文件
print(client.listdir("/HadoopFileS/DataSet"))

# 打开一个远程节点上的文件，返回一个HTTPResponse对象
response = client.open("/HadoopFileS/DataSet/Others/My_Internship_Experience.txt")
# 查看文件内容
print(response.read())