# %%
import requests
import re
import json
import time
import csv

page = int(input("输入爬取的页数总数"))
choose = str(input("是否保存(保存输入yes)"))

if (choose == "yes"):
    f = open("/root/Github_files/python_All/Dataset/51job_java.csv", mode='a', encoding='utf-8', newline='')
    csv_writer = csv.DictWriter(f, fieldnames=[
        "职位名字",
        "公司名字",
        "公司类型",
        "薪资",
        "地区",
        "工作年限",
        "发布日期",
        "福利",
        "经验",
        "学历",
        "所属行业",
    ])
    csv_writer.writeheader()  # 写入表头

# 生成csv的表头
for i in range(1, page + 1):
    print("正在写入第%d页" % (i))
    time.sleep(2.0)  # 防止访问过快被识别拒绝
    # 下面为大数据开发工程师（成都）
    # url = f"https://search.51job.com/list/090200,000000,0000,00,9,99,%25E5%25A4%25A7%25E6%2595%25B0%25E6%258D%25AE%25E5%25BC%2580%25E5%258F%2591%25E5%25B7%25A5%25E7%25A8%258B%25E5%25B8%2588,2,{i}.html?"
    # 下面为算法工程师（成都）
    # url = f"https://search.51job.com/list/090200,000000,0000,00,9,99,%25E7%25AE%2597%25E6%25B3%2595%25E5%25B7%25A5%25E7%25A8%258B%25E5%25B8%2588,2,{i}.html?"
    # 下面为python岗位（成都）
    # url = f"https://search.51job.com/list/090200,000000,0000,00,9,99,python,2,{i}.html"
    # 虾米那为java岗位（成都）
    url = f"https://search.51job.com/list/090200,000000,0000,00,9,99,java,2,{i}.html"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.54 Safari/537.36"
    }
    # 调用get请求
    response = requests.get(url=url, headers=headers)
    print(response)  # 200正常相应
    ##response.text
    # %%解析数据使用re正则表达式
    html_data = re.findall("window.__SEARCH_RESULT__ =(.*?)</script>", response.text)
    print(type(html_data))
    ##import json
    json_data = json.loads(html_data[0])['engine_jds']
    ##print(json_data)
    print(type(json_data))
    ##json_data
    # %%提取数据
    for info in json_data:
        title = info['job_title']  # 岗位
        company_name = info['company_name']  # 公司
        salary = info['providesalary_text']  # 薪资
        companytype = info['companytype_text']  # 性质
        region = info['workarea_text']  # 地区
        workyear = info['workyear']  # 年限
        issuedate = info['issuedate']
        # enddate = info['enddate']
        jobwelf = info['jobwelf']
        if (len(info['attribute_text']) >= 3):
            exp = info['attribute_text'][1]
            edu = info['attribute_text'][2]
        else:
            exp = info['attribute_text'][1]
            edu = None
        companyind_text = info['companyind_text']

        # %%生成csv的表头
        dicts = {
            "职位名字": title,
            "公司名字": company_name,
            "公司类型": companytype,
            "薪资": salary,
            "地区": region,
            "工作年限": workyear,
            "发布日期": issuedate,
            "福利": jobwelf,
            "经验": exp,
            "学历": edu,
            "所属行业": companyind_text,
        }
        csv_writer.writerow(dicts)