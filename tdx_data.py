

#  通达信数据存储
# ### 基本设置
# ####  导入模块

import pandas as pd
import numpy as np
from pathlib import Path
import re
import platform

# 设置hdf5默认存储格式-允许put/append/to_hdf
pd.set_option('io.hdf.default_format', 'table')

# 设置数据目录
# + windows通达信程序主目录(其vipdoc子目录中包含日线数据)  D:/2020tdx
# + windosw股票数据主目录 D:/Stock_Data其下的csv和hdf子目录分别存储对应格式的数据文件
# + Tushare相关指标数据存于子目录tushare中
# + mac及ubuntu均为主目录下建立对应的子目录


if(platform.system() == 'Windows'):
    TDX_Data_Dir = Path('D:/2020tdx/vipdoc')
    Stock_Data_Dir = Path('D:/Stock_Data')
else:
    TDX_Data_Dir = Path.home()/'Stock_Data/TDX'
    Stock_Data_Dir = Path.home()/'Stock_Data'
sub_dirs = ['csv', 'hdf', 'tushare']

for i in range(len(sub_dirs)):
    out_sub_dir = Stock_Data_Dir/sub_dirs[i]  # 可以使用/符号来拼接路径
    if(not out_sub_dir.exists()):
        out_sub_dir.mkdir()
    print(out_sub_dir)


# 判断是否为A股代码


def judge_stockcode(dayfile):
    '''
    传入通达信文件名，根据正则表达式判断其是否为A股

    深市A股的代码是00和300开头的,其中000开头的是主板股票，002开头的是中小板股票，300开头的是创业板股票。
    沪市A股的代码是以6开头的，其中600、601或603是主板股票，688是是科创板股票
    '''
    filename = dayfile.stem  # 读取主文件名，不含路径和扩展名
    pattern = '(sh600|sh601|sh603|sh688|sz000|sz002|sz300)\d{3}$'
    regex = re.compile(pattern, flags=re.I)
    Astockcode = True if regex.match(filename) else False
    return Astockcode


# ##### 解析文件数据


def parse_data(dayfile, fileoffset=0):
    '''
    通达信数据文件每32字节为一天的交易数据，其中每4个字节为一个字段内容，每个字段采用小端存储，
    低字节在前面，包含交易日期、ohlc、amount、vol等7个有效数据字段,最后4个字节为保留字段

    00 ~ 03 字节：年月日,整型  trade_date
    04 ~ 07 字节：开盘价*100,整型 open
    08 ~ 11 字节：最高价*100,整型 high
    12 ~ 15 字节：最低价*100,整型 low
    16 ~ 19 字节：收盘价*100, 整型 close
    20 ~ 23 字节：成交额（元）,float型--/1000=千元 amount
    24 ~ 27 字节：成交量（股）,整型--/100=手  vol 
    28 ~ 31 字节：（保留）
    '''
    with open(dayfile, 'rb') as f:
        data = np.fromfile(
            f, dtype='<i4,<i4,<i4,<i4,<i4,<f4,<i4,<i4', offset=fileoffset)  # 直接读入二进制结构数组中
    kline = [list(data[i])[:7] for i in range(data.size)]
    return kline


# ##### 构建Tushare结构的股票数据


def build_tushare_data(kline, tdxcols, tusharecols, ts_code):
    '''
    读取通达信A股数据文件，参考Tushare日线行情daily接口的输出参数名和顺序构建股票数据

    '''
    sdata = pd.DataFrame(data=kline, columns=tdxcols)
    sdata['trade_date'] = sdata['trade_date'].apply(
        lambda x: str(x))  # 将整型日期转换为字符型
    sdata['open'] = sdata['open'] / 100
    sdata['high'] = sdata['high'] / 100
    sdata['low'] = sdata['low'] / 100
    sdata['close'] = sdata['close'] / 100
    sdata['amount'] = sdata['amount'] / 1000
    sdata['vol'] = sdata['vol'] / 100

    sdata = sdata.reindex(columns=tusharecols)
    sdata['ts_code'] = ts_code  # 按Tushare格式构造股票代码
    sdata.set_index('trade_date', inplace=True)  # 将交易时间设置为索引，方便后续增补数据
    # sdata.index=pd.DatetimeIndex(sdata.index) #与Tushare格式不一致
    return sdata


# #####  遍历日线文件并存储


filesize = {}  # 用字典记录通达信日线文件的长度
hdffile = Stock_Data_Dir / 'hdf/t6.h5'
if(not Path(hdffile).exists()):  # 如果hdf文件不存在，则为全备份
    fileoffset = 0  # 字节偏移量为0
    dayfileinfo = False
else:
    dayfileinfo = True  # 如果hdf文件存在，则为增量备份

# 据说使用blosc速度最快，但HDFView打开时数据报错
tdx_stack = pd.HDFStore(str(hdffile), complevel=9)
filecount = 0

tdxcols = ['trade_date', 'open', 'high', 'low', 'close', 'amount', 'vol']
tusharecols = [
    'ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'vol', 'amount'
]
# 数据目录vipdoc下没有包含.day名的子目录，否则要进行判断
for dayfile in TDX_Data_Dir.rglob('*.day'):
    if (judge_stockcode(dayfile)):  # 如果是A股则提取数据
        filelength = dayfile.stat().st_size  # 获取文件长度
        if (filelength % 32):  # 文件的长度如果不是32的倍数，则继续循环下一个
            print('文件{}有损坏，请检查'.format(dayfile))
            continue
        else:
            filesize[dayfile.name] = filelength  # 记录通达信日线文件的长度
            code_label = dayfile.stem  # 设置group为主文件名
            ts_code = dayfile.stem[2:] + '.' + \
                dayfile.stem[:2].upper()  # 按Tushare格式构造股票代码
            if(dayfileinfo):  # 检查对应股票代码文件长度
                if(dayfile in tdx_stack['dayfileinfo']):  # 如果有该股票，检查文件--避免停牌
                    if(filelength > tdx_stack['dayfileinfo'][dayfile]):
                        fileoffset = tdx_stack['dayfileinfo'][dayfile]
                        tp = False  # 当日未停牌
                    else:
                        tp = True  # 当日停牌
                else:  # 当日新股上市
                    offset = 0
            if(not suspension):
                kline = parse_data(dayfile, fileoffset)  # 解析数据
                tdx_df = build_tushare_data(kline, tdxcols, tusharecols,
                                            ts_code)  # 构建股票数据
                tdx_stack.put(code_label, tdx_df, append=True)
                filecount += 1

tdx_stack.put('dayfileinfo', pd.Series(filesize))
tdx_stack.close()
print(filecount)


tdx_stack.close()


# ##### 查看数据基本情况


tdx_stack = pd.HDFStore(str(Stock_Data_Dir/'hdf/tdx.h5'))
test = tdx_stack['sh600000'].head(10)
test

tdx_stack.close()


test.info()
