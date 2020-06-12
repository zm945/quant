'''
@Author: 巴山哥
@Date: 2020-06-07 10:43:19
@Description： 自选股分价统计
@FilePath: \quant\vol_price.py
@E-mail: zm945@126.com
'''
import pandas as pd
from pathlib import Path
import re
import platform

# 设置hdf5默认存储格式-允许put/append/to_hdf
pd.set_option('io.hdf.default_format', 'table')

# 设置数据目录
# 根据操作系统分别设置不同的数据目录了
# TDX_Data_Dir--通达信原始日线数据目录
# Stock_Data_Dir--存放转换后的数据主目录
# sub_dirs--存储转换后对应格式的数据文件

if (platform.system() == 'Windows'):
    Vp_Data_Dir = Path('D:/2020tdx/T0002/export/')  #分价表原始数据子目录
    Vol_Price_Dir = Path('D:/Stock_Data/Vol_Price') #分价数据存储目录
else:
    TDX_Data_Dir = Path.home() / 'Stock_Data/TDX'  # mac及ubuntu
    Vp_Data_Dir = TDX_Data_Dir / 'export'  #分价表原始数据子目录
    Vol_Price_Dir = Path.home() / 'Stock_Data/Vol_Price'
if (not Vol_Price_Dir.exists()):
        Vol_Price_Dir.mkdir(parents=True, exist_ok=True)

hdffile = Vol_Price_Dir / 'vp.h5'
if (not Path(hdffile).exists()):  # 如果hdf文件不存在，则为全备份
    updata = False
    tip = '新建数据文件.....'
else:
    updata = True  # 如果hdf文件存在，则为增量备份
    tip = '增量备份文件....'
vp=[]
for vpfile in Vp_Data_Dir.rglob('*.txt'):
    if('分价' in vpfile.stem):
        with open(vpfile,'r',encoding='GB2312') as f:
            t=f.readline().split()
            trade_date=t[0]
            name=t[1]
            code=t[2][1:-1]
            df=pd.read_csv(vpfile,header=1,encoding='GB2312',sep='\s+',skipfooter=1, engine='python')
            df['ts_code']=code
            df['name']=name
            df['trade_date']=trade_date
            vp.append(df)
            
vol_price=pd.concat(vp,ignore_index=True)
print(tip)
vol_price.to_hdf(hdffile, 'vol_price', append=updata,encoding='GB2312')
print('ok')
