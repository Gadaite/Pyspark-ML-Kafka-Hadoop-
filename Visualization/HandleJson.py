import json
import pprint
data = json.load(open("/root/china.json", encoding='utf-8'))
res = str(data)
print(res)
dict = json.loads(res)
# print(dict)
