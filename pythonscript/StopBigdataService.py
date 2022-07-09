import os

#   Spark停止
print("----------------Spark----------------------------")
os.system("source /opt/spark-2.4.5-bin-hadoop2.7/sbin/stop-all.sh")
print("\n")

#   Flink停止
print("----------------Flink----------------------------")
os.system("/opt/flink-1.6.3/bin/stop-cluster.sh")
print("\n")

#   Habse停止
print("----------------Hbase----------------------------")
os.system("source /opt/hbase-2.1.1/bin/stop-hbase.sh")
print("\n")

#   Hive启动远程服务并守护进程
print("----------------Hive,HiveServer2----------------------------")
os.system("source /opt/apache-hive-2.3.9-bin/bin/stop-hiveserver2.sh")
#   使用stty代替ctrl+C退出终端
os.system("stty")
print("\n")

#   停止Hadoop
print("----------------Hadoop(Hdfs)----------------------------")
os.system("source /opt/hadoop-2.7.7/sbin/stop-all.sh")
os.system("pwd")
print("\n")

#   KafKa停止
print("----------------Kafka----------------------------")
os.system("source /opt/kafka_2.11-2.4.0/bin/kafka-server-stop.sh")
os.system("stty")
print("\n")

#   Zookeeper停止
print("----------------Zookeeper----------------------------")
os.system("/opt/zookeeper-3.4.14/bin/zkServer.sh stop")
os.system("stty")
print("\n")

#   查看启动服务情况
print("---------------JPS(Result)-----------------------")
os.system("jps")
print("\n")
print("---------------Docker(Result)-----------------------")
os.system("docker ps")
print("\n")
print("---------------JPS(Detailed Result)-----------------------")
os.system("jps -l")
print("\n")
print("-------Docker中数据库并未停止，如果需要，手动停止即可-----------------------")
