import os
#   停止Kibana7
print("----------------Kibana7,****root(Username)****----------------------------")
os.system("bash /opt/kibana-7.10.2-linux-x86_64/bin/stopkibana.sh")
print("\n")

#   停止ElasticSearch7
print("----------------ElasticSearch7,****root(Username)****-----------------------")
os.system("bash /opt/elasticsearch-7.10.2/bin/stopelasticsearch.sh")
print("\n")

