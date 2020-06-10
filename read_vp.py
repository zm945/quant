# -*- coding: utf-8 -*-
'''
@Author: 巴山哥
@Date: 2020-06-09 09:53:32
@LastEditTime: 2020-06-09 15:33:52
@Description: 读取通达信分价统计数据
@FilePath: \quant\read_vp.py
@E-mail: zm945@126.com
'''

import pandas as pd
from pathlib import Path
import re
import platform

if (platform.system() == 'Windows'):
    Vol_Price_Dir = Path('D:/Stock_Data/Vol_Price')  # 分价数据存储目录
else:
    Vol_Price_Dir = Path.home() / 'Stock_Data/Vol_Price'

vpfile = Vol_Price_Dir/'vp.h5'  #分价数据文件
df = pd.read_hdf(vpfile)
for code in set(df['ts_code']):
    vp=df.loc[df.ts_code==code]
    days = set(vp.trade_date)
    datalist = []
    for tdate in days:
        tt = vp[vp.trade_date == tdate]
        tmp = tt.sort_values(by='成交').tail()
        datalist.append(tmp)
    vdata = pd.concat(datalist, ignore_index=True)
    print(code,'最近几日成交情况:')
    #print(vdata.sort_values(by='成交'))
    vdata['trade_date']=pd.to_datetime(vdata['trade_date'])
    print(vdata.sort_values(by='trade_date'))
    print('汇总:')
    print(vdata.groupby('价格').sum().sort_values(by='成交'))
    print('-------------------------------------------------------')

