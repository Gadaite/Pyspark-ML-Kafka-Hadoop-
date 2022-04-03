import pandas as pd
from sqlalchemy import create_engine
engine = create_engine('mysql+pymysql://root:LYP809834049@192.168.1.10:3306/Gadaite')
print(engine)
sql = 'select * from seeds_dataset'
df = pd.read_sql_query(sql, engine)
print(df)