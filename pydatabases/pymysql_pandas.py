import pymysql
import pandas as pd
conn = pymysql.connect(host='192.168.1.10'\
                       ,database='Gadaite',user="root",passwd="LYP809834049")
sql1 = "show tables"
df_tables = pd.read_sql(sql1,conn)
print(df_tables)

sql2 = "select * from seeds_dataset limit 5"
tables_seeds = pd.read_sql(sql2,conn)
print(tables_seeds)
conn.close()