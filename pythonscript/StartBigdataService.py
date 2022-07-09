import os

#   docker 启动数据库
print("----------------Docker(Mysql,PostgresSql,Oracle,MongoDB)----------------------------")
os.system("docker start mysql")
os.system("docker start postgis")
os.system("docker start oracle")
os.system("docker start mongodb_server")
print("\n")

#   启动Hadoop
print("----------------Hadoop(Hdfs)----------------------------")
os.system("source /opt/hadoop-2.7.7/sbin/start-all.sh")
os.system("pwd")
print("\n")

#   hdfs退出安全模式
print("----------------关闭HDFS安全模式------------------------")
os.system("hdfs dfsadmin -safemode leave")
print("\n")

#   Hive启动远程服务并守护进程
print("----------------Hadoop(Hive,HiveServer2)----------------------------")
os.system("nohup /opt/apache-hive-2.3.9-bin/bin/hiveserver2 &")
#   使用stty代替ctrl+C退出终端
os.system("stty")
print("\n")

#   Zookeeper启动并守护进程
print("----------------Zookeeper----------------------------")
os.system("/opt/zookeeper-3.4.14/bin/zkServer.sh start")
os.system("stty")
print("\n")

#   KafKa启动并守护进程
print("----------------Kafka----------------------------")
os.system("nohup /opt/kafka_2.11-2.4.0/bin/kafka-server-start.sh /opt/kafka_2.11-2.4.0/config/server.properties 2>&1 &")
os.system("stty")
print("\n")

#   Habse启动并守护进程
print("----------------Hbase----------------------------")
os.system("source /opt/hbase-2.1.1/bin/start-hbase.sh")
print("\n")

#   Spark启动
print("----------------Spark----------------------------")
os.system("source /opt/spark-2.4.5-bin-hadoop2.7/sbin/start-all.sh")
print("\n")

#   Flink启动
print("----------------Flink----------------------------")
os.system("/opt/flink-1.6.3/bin/start-cluster.sh start")
print("\n")

#   查看启动服务情况
print("---------------JPS(Result)-----------------------")
os.system("jps")
print("\n\n")
print("---------------Docker(Result)-----------------------")
os.system("docker ps")
print("\n\n")
print("---------------JPS(Detailed Result)-----------------------")

os.system("jps -l")
print("\n\n")
print("-------启动ElasticSearch,Kibana等请执行StartELKService.py脚本-----------------------")
