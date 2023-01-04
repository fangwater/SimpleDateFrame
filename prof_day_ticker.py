import cProfile, pstats, io

import pandas as pd
import numpy as np
import os,os.path
from joblib import Parallel, delayed
import warnings
from dateutil.relativedelta import relativedelta
import collections.abc
import datetime

date = f"2022-12-06"
date = pd.to_datetime(date)

pr = cProfile.Profile()
pr.enable()
hfq_multi_daily = pd.read_parquet(f'./hfq_multi.parquet').reindex(index=[date]).fillna(method='ffill').replace(0,np.nan)

def resample_day_ticker(ticker,date):
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore')
        temp_path = './20221206'
        if os.path.exists(f'{temp_path}/{ticker}.parquet'):
            aa = pd.read_parquet(f'{temp_path}/{ticker}.parquet')
        else:
            return pd.DataFrame()
        if aa.empty:
            return pd.DataFrame()
        # array of time_stramp, date + time tick
        # can be faster
        aa['datetime'] = pd.to_datetime(aa['date'].apply(str)+' '+aa['updatetime'].apply(str))
        # select
        aa = aa.loc[aa.tradp>0]
        # 
        aa['tradamt'] = aa['tradv'] * aa['tradp']
        aa_B = aa.loc[aa.bs == 'B']
        aa_S = aa.loc[aa.bs == 'S']
        # a lot of groupby sampleing at the same index
        res = aa.groupby([pd.Grouper(key='datetime',freq=f'T',closed='right',label='right',dropna=False),'securityid'])['tradp'].count().to_frame()
        res.columns = ['cjbs']
        res['bcjbs'] = aa_B.groupby([pd.Grouper(key='datetime',freq=f'T',closed='right',label='right',dropna=False),'securityid'])['tradp'].count()
        res['scjbs'] = aa_S.groupby([pd.Grouper(key='datetime',freq=f'T',closed='right',label='right',dropna=False),'securityid'])['tradp'].count()
        res['volume_nfq'] = aa.groupby([pd.Grouper(key='datetime',freq=f'T',closed='right',label='right',dropna=False),'securityid'])['tradv'].sum()
        res['bvolume_nfq'] = aa_B.groupby([pd.Grouper(key='datetime',freq=f'T',closed='right',label='right',dropna=False),'securityid'])['tradv'].sum()
        res['svolume_nfq'] = aa_S.groupby([pd.Grouper(key='datetime',freq=f'T',closed='right',label='right',dropna=False),'securityid'])['tradv'].sum()
        res['amount'] = aa.groupby([pd.Grouper(key='datetime',freq=f'T',closed='right',label='right',dropna=False),'securityid'])['tradamt'].sum()
        res['bamount'] = aa_B.groupby([pd.Grouper(key='datetime',freq=f'T',closed='right',label='right',dropna=False),'securityid'])['tradamt'].sum()
        res['samount'] = aa_S.groupby([pd.Grouper(key='datetime',freq=f'T',closed='right',label='right',dropna=False),'securityid'])['tradamt'].sum()
        res['closeprice_nfq'] = aa.groupby([pd.Grouper(key='datetime',freq=f'T',closed='right',label='right',dropna=False),'securityid'])['tradp'].last()
        res['openprice_nfq'] = aa.groupby([pd.Grouper(key='datetime',freq=f'T',closed='right',label='right',dropna=False),'securityid'])['tradp'].first()
        res['highprice_nfq'] = aa.groupby([pd.Grouper(key='datetime',freq=f'T',closed='right',label='right',dropna=False),'securityid'])['tradp'].max()
        res['lowprice_nfq'] = aa.groupby([pd.Grouper(key='datetime',freq=f'T',closed='right',label='right',dropna=False),'securityid'])['tradp'].min()
        return res.reset_index()

tmp = []
count = 0
for ticker in hfq_multi_daily.columns:
    count = count+1
    tmp.append(resample_day_ticker(ticker,date))
    if count == 2:
        break
res = pd.concat(tmp,axis=0,copy=False)

pr.disable()
s = io.StringIO()
sortby = "cumtime"  # 仅适用于 3.6, 3.7 把这里改成常量了
ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
ps.print_stats()
pr.dump_stats("day_ticker.prof")