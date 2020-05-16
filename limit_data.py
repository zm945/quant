#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#############################################################################
#     File Name: limit_data.py
#        Author: 巴山哥
#        E-mail: zm945@126.com
#    Created on: Sat May 16 22:12:21 2020
#   Description: 读取Tushare数据建立涨跌停数据库
#   Code editor: Spyder & vscode
#############################################################################

# 导入相关模块
import tushare as ts
import pandas as pd
import numpy as np
from pathlib import Path
import pickle
import datetime as dt
import time

# 根据涨跌停函数及交易日历构建数据


def build_data(func, dates):
    tmp = []
    for tdate in dates:
        while (True):
            try:
                df = func(trade_date=tdate)
                tmp.append(df)
                break
            except:
                print('因超时等待10秒重试')
                time.sleep(10)
    limitdatas = pd.concat(tmp, ignore_index=True)
    return limitdatas


# 设置数据目录及数据文件
Stock_Data_Dir = Path.home() / 'Stock_Data/Tushare'
if (not Stock_Data_Dir.exists()):
    Stock_Data_Dir.mkdir(parents=True, exist_ok=True)
out_hdf_dir = Stock_Data_Dir / 'hdf'
if (not out_hdf_dir.exists()):  # Path.exists()
    out_hdf_dir.mkdir(parents=True, exist_ok=True)
hdfile = out_hdf_dir / 'stock.h5'  # 数据库文件

# 设置token并初始化接口
tokenfile = Stock_Data_Dir / 'Token' / 'token.pk'
with open(tokenfile, 'rb') as f:
    mytoken = pickle.load(f)
ts.set_token(mytoken)
pro = ts.pro_api()

# 设置hdf文件存储格式
pd.set_option('io.hdf.default_format', 'table')

# 判断数据库文件是否存在
if (Path.exists(hdfile)):
    updata = True  # 如果文件存在则采用追加更新
    rdate = pd.read_hdf(hdfile, key='recordlastdate')  # 读取库中记录的最后统计时间
    lsdate = rdate['trade_date']
    stkdate = rdate['trade_date']
    sindex = 1  # 日期切片索引起始位置，1-当前最后统计日期的下一个交易日
else:  # 新建数据文件，从头至今存储所有数据
    updata = False
    lsdate = '20160215'  # 涨跌停统计数据起始时间为20160215
    stkdate = '20070104'  # 单日全部股票数据涨跌停价格起始时间为20070104
    sindex = 0

# 获取当前时间
now = dt.datetime.now()  # 输出顺序为：年、月、日、时、分、秒、微妙
today = dt.date.today()
edate = today.strftime("%Y%m%d")  # 转换时间格式
print('today=', edate)

# 获取各大交易所交易日历数据,默认提取的是上交所
limit_cal = pro.trade_cal(exchange='', start_date=lsdate,
                          end_date=edate, is_open='1')['cal_date'][sindex:]
if(sindex):  # 追加更新数据-limit和stk用的是同一个日历数据
    stk_cal = limit_cal
else:  # 如果是新建数据库，limit和stk的起始时间不同，必须另外获取
    stk_cal = pro.trade_cal(exchange='', start_date=stkdate,
                            end_date=edate, is_open='1')['cal_date'][sindex:]

# 获取单日涨跌停统计数据，起始时间为20160215
# limit_list = pro.limit_list(trade_date='20160215')
# 获取单日全部股票数据涨跌停价格-经测试确定，起始时间为20070104
# stk_limit= pro.stk_limit(trade_date='20070104')
# 限量：单次最多提取4800条记录，可循环调取，总量不限制
# 积分：用户积600积分可调取，单位分钟有流控，积分越高流量越大，

if len(stk_cal) > 0:
    rdate = stk_cal.iloc[-1]  # 记录最后的统计日期
    limit_list = build_data(pro.limit_list, limit_cal)
    limit_list.to_hdf(hdfile, 'limit_list', append=updata,
                      complevel=9)   # 在win10上用hdfview查看会乱码，必须设置encoding='gbk'
    print('获取单日涨跌停统计数据ok')
    stk_limit = build_data(pro.stk_limit, stk_cal)
    stk_limit.to_hdf(hdfile, 'stk_limit', append=updata,
                     complevel=9)
    print('获取单日全部股票数据涨跌停价格ok')
    pd.Series({'trade_date': rdate}).to_hdf(hdfile, 'recordlastdate')
    print('记录最后统计日期ok')
else:
    print('已经是最新数据，无需更新')
