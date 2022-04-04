#%%
import pandas as pd
import numpy as np
#%%
# 以三天时间作为一个周期
data = pd.date_range("2022/04/04",periods=60,freq="3D")
ts = pd.Series(np.random.randn(60),index=data)
ts
#%%
#按照月份进行降采样
dfs = ts.resample('M').sum()
dfs
#%%
#升采样会出现空值的情况，这里就需要进行一些值得填充
# 出现的是NaN，asfreq换成sum会变成0
dfs1 = ts.resample('D').asfreq()
dfs1
#%%
#空值取前面的值
ts.resample('D').ffill(1)
#%%
#空值取后面的值
ts.resample('D').bfill(2)
#%%
#空值使用线性取值
ts.resample('D').interpolate('linear')