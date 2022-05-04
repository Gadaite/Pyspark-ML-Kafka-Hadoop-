from pyspark.sql import SparkSession
import json
import requests
import re
import pprint
import pandas as pd
"""
    未完成，期望使用爬虫requests模块读取Elastic数据
    转换为SparkDataframe进行查看
"""
spark = SparkSession.builder.config("spark.driver.host","192.168.1.10")\
    .config("spark.ui.showConsoleProgress","false").master("local[*]")\
    .appName("ESdata").getOrCreate()
body = \
{"query": {"match_all": {}},
  "from": 0,
  "size": 10000,
  "sort": [{"account_number": {"order": "desc"}}]
}
headers = {'content-type': "application/json"}
response = requests.get(url="http://192.168.1.10:9200/bank/_search",data=json.dumps(body),headers=headers)
elasticsearch_json_data = json.loads(re.findall('"hits":\[(.*?)\]}}',response.text)[0])["_source"]
# elasticsearch_json_data = re.findall('"hits":\[(.*?)\]}}',response.text)[0]
print(elasticsearch_json_data)



