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
                if (not df.empty):  # 未收盘时，取不到当天到统计数据
                    tmp.append(df)
                break
            except:
                print('因超时等待10秒重试')
                time.sleep(10)
    if (len(tmp) > 0):
        limitdatas = pd.concat(tmp, ignore_index=True)
    else:
        limitdatas = pd.DataFrame()
    return limitdatas


# 设置数据目录及数据文件
Stock_Data_Dir = Path.home() / 'Stock_Data/Tushare'
if (not Stock_Data_Dir.exists()):
    Stock_Data_Dir.mkdir(parents=True, exist_ok=True)
out_hdf_dir = Stock_Data_Dir / 'hdf'
if (not out_hdf_dir.exists()):
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
    lsdate = rdate['lsdate']  # 涨跌停统计数据的最后时间
    stkdate = rdate['stkdate']
    sindex = 1  # 日期切片索引起始位置，1--当前最后统计日期的下一个交易日
else:  # 新建数据文件，从头至今存储所有数据
    updata = False
    lsdate = '20160215'  # 涨跌停统计数据起始时间为20160215
    stkdate = '20070104'  # 单日全部股票数据涨跌停价格起始时间为20070104
    sindex = 0

# 获取当前时间
now = dt.datetime.now()  # 输出顺序为：年、月、日、时、分、秒、微妙
today = dt.date.today()
edate = today.strftime("%Y%m%d")  # 转换时间格式
#edate = '20200520'
print('当前日期是:', edate)

# 获取各大交易所交易日历数据,默认提取的是上交所
limit_cal = pro.trade_cal(exchange='',
                          start_date=lsdate,
                          end_date=edate,
                          is_open='1')['cal_date'][sindex:]
stk_cal = pro.trade_cal(exchange='',
                        start_date=stkdate,
                        end_date=edate,
                        is_open='1')['cal_date'][sindex:]

# 获取单日涨跌停统计数据
# limit_list = pro.limit_list(trade_date='20160215')
# 获取单日全部股票数据涨跌停价格-经测试确定
# stk_limit= pro.stk_limit(trade_date='20070104')
# 限量：单次最多提取4800条记录，可循环调取，总量不限制
# 积分：用户积600积分可调取，单位分钟有流控，积分越高流量越大，
tradedate = {}  #构建各数据集时间标签字典，{},[],()等都等价于False！
if len(stk_cal) > 0:
    stkdate = stk_cal.iloc[-1]  # 记录最后的统计日期
    tradedate['stkdate'] = stkdate
    stk_limit = build_data(pro.stk_limit, stk_cal)
    if (not stk_limit.empty):
        stk_limit.to_hdf(hdfile, 'stk_limit', append=updata, complevel=9)
        print('获取单日全部股票数据涨跌停价格-ok!,当前最后交易日期为:{}'.format(stkdate))
if (len(limit_cal) > 0):
    lsdate = limit_cal.iloc[-1]  # 记录最后的统计日期
    tradedate['lsdate'] = lsdate
    limit_list = build_data(pro.limit_list, limit_cal)
    if (not limit_list.empty):
        limit_list.to_hdf(
            hdfile, 'limit_list', append=updata,
            complevel=9)  # 在win10上用hdfview查看会乱码，必须设置encoding='gbk'
        print('获取单日涨跌停统计数据-ok!,当前最后交易日期为:{}'.format(lsdate))
if (tradedate):  # 非空
    pd.Series(tradedate).to_hdf(hdfile, 'recordlastdate')
    print('记录最后统计日期-ok!')
else:
    print('全部股票数据涨跌停价格最后记录日期为:{}'.format(stkdate))
    print('涨跌停统计数据最后记录日期为:{}'.format(lsdate))
    print('已经是最新数据，无需更新')
