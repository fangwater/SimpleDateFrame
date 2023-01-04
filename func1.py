import pandas as pd
import numpy as np
import os,os.path
from joblib import Parallel, delayed
import warnings
from dateutil.relativedelta import relativedelta
import collections.abc
import datetime

import cProfile,pstats,io

data_path = "./20221206/000001.XSHE.parquet"
aa = pd.read_parquet(f'{data_path}')
aa['datetime'] = pd.to_datetime(aa['date'].apply(str)+' '+aa['updatetime'].apply(str))
# select
aa = aa.loc[aa.tradp>0]
# 
aa['tradamt'] = aa['tradv'] * aa['tradp']
aa_B = aa.loc[aa.bs == 'B']
aa_S = aa.loc[aa.bs == 'S']
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
ans = res.reset_index()