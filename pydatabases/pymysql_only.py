import pymysql
host = '192.168.1.10'
user = 'root'
password = 'LYP809834049'
database = 'Gadaite'
# 连接数据库
db = pymysql.connect(host=host,user=user,password=password,database=database)

# cursor() 方法创建一个游标对象 cursor
curor = db.cursor()

# execute()  方法执行SQL
sql1 = "show tables"
sql2 = "select * from seeds_dataset"
print(curor.execute(sql1)) # 这样其实是输出对应结果的行数
# 列出结果集(数据库中的表)
for i in curor.fetchall():
    print(i) # i 代表数据的每一行数据
print(curor.execute(sql2)) # 这样其实是输出对应结果的行数，同时游标也发生相应的变化
# 列出结果集(该表中的数据)
for i in curor.fetchall():
    print(i) # i 代表数据的每一行数据

# 提交
db.commit()

# 关闭游标
curor.close()
# 关闭数据库连接
db.close()