#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#############################################################################
#     File Name: limit_data.py
#        Author: 巴山哥
#        E-mail: zm945@126.com
#    Created on: Thu May  7 09:17:31 2020
#   Description: 读取Tushare数据建立涨跌停数据库
#   Code editor: Spyder 编辑器
#############################################################################

# 导入相关模块
import tushare as ts
import pandas as pd
import numpy as np
from pathlib import Path
import pickle
import datetime as dt
import time

# 设置数据目录
Stock_Data_Dir = Path.home()/'Stock_Data/Tushare'
if (not Stock_Data_Dir.exists()):
    Stock_Data_Dir.mkdir(parents=True, exist_ok=True)
out_hdf_dir = Stock_Data_Dir/'hdf'
if(not out_hdf_dir.exists()):  # Path.exists()
    out_hdf_dir.mkdir(parents=True, exist_ok=True)

# 设置token并初始化接口
tokenfile = Stock_Data_Dir/'Token'/'token.pk'
with open(tokenfile, 'rb') as f:
    mytoken = pickle.load(f)
ts.set_token(mytoken)
pro = ts.pro_api()

# 设置hdf文件存储格式
pd.set_option('io.hdf.default_format', 'table')

# 获取单日涨跌停统计数据，起始时间为20160215
# limit_list = pro.limit_list(trade_date='20160215')

# 获取单日全部股票数据涨跌停价格-经测试确定，起始时间为20070104
# 当日股票涨跌停价格。
# 限量：单次最多提取4800条记录，可循环调取，总量不限制
# 积分：用户积600积分可调取，单位分钟有流控，积分越高流量越大，
# stk_limit= pro.stk_limit(trade_date='20070104')

# 获取各大交易所交易日历数据,默认提取的是上交所
lsdate = '20160215'  # 涨跌停统计数据起始时间为20160215
stkdate = '20070104'  # 单日全部股票数据涨跌停价格起始时间为20070104
edate = '20200506'   #设置截止时间
limit_cal = pro.trade_cal(exchange='', start_date=lsdate,
                          end_date='20200506', is_open='1')['cal_date']
stk_cal = pro.trade_cal(exchange='', start_date=stkdate,
                        end_date='20200506', is_open='1')['cal_date']

def build_data(func, dates):
    tmp = []
    for tdate in dates:
        while(True):
            try:
                df = func(trade_date=tdate)
                tmp.append(df)
                break
            except:
                print('因超时等待10秒重试')
                time.sleep(10)
    limitdatas = pd.concat(tmp, ignore_index=True)
    return limitdatas

               
limit_list=build_data(pro.limit_list, limit_cal)
limit_list.to_hdf(out_hdf_dir/'hdf.h5', 'limit_list',
                 complevel=9)   # 在ubuntu上用hdfview查看会乱码，必须设置encoding='gbk'
print('获取单日涨跌停统计数据ok') 

stk_limit=build_data(pro.stk_limit, limit_cal)
stk_limit.to_hdf(out_hdf_dir/'hdf.h5', 'stk_limit',
                 complevel=9)   # 在ubuntu上用hdfview查看会乱码，必须设置encoding='gbk'
print('获取单日全部股票数据涨跌停价格ok') 




