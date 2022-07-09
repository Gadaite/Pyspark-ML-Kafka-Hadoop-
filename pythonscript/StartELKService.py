import os
import time
#   启动ElasticSearch7
print("----------------ElasticSearch7,****elk(Username)****-----------------------")
os.system("/opt/elasticsearch-7.10.2/bin/elasticsearch -d")
time.sleep(15)
os.system("stty")
print("\n")

#   启动Kibana7
print("----------------Kibana7,****elk(Username)****----------------------------")
os.system("nohup  /opt/kibana-7.10.2-linux-x86_64/bin/kibana &")
time.sleep(15)
os.system("stty")
print("\n")
