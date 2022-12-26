import requests
responseHtml = requests.request(method="GET",url="http://poilist.cn/poi-list-%E6%99%AF%E7%82%B9-%E6%88%90%E9%83%BD/0/")
print(responseHtml.text)
import json
json_data = json.load(responseHtml[0])
print(json_data)