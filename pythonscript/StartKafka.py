import os
#   KafKa可能未启动，再启动一次Kafka
print("----------------Kafka Try Again----------------------------")
os.system("nohup /opt/kafka_2.11-2.4.0/bin/kafka-server-start.sh /opt/kafka_2.11-2.4.0/config/server.properties 2>&1 &")
os.system("stty")
print("\n")