#!/usr/bin/env python
# coding: utf-8

# function_name:resample_day
# args: 
# - date
# - n_jobs

# In[1]:
import cProfile, pstats, io

import pandas as pd
import numpy as np
import os,os.path
from joblib import Parallel, delayed
import warnings
from dateutil.relativedelta import relativedelta
import collections.abc
import datetime


# In[4]:


date = f"2022-12-06"
date = pd.to_datetime(date)


# In[5]:

pr = cProfile.Profile()
pr.enable()
hfq_multi_daily = pd.read_parquet(f'./hfq_multi.parquet').reindex(index=[date]).fillna(method='ffill').replace(0,np.nan)


# In[8]:


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


# In[9]:


res = pd.concat(Parallel(n_jobs=16,verbose=5,pre_dispatch='all',batch_size=500,backend='loky')(delayed(resample_day_ticker)(ticker,date) for ticker in hfq_multi_daily.columns),axis=0,copy=False)


# In[11]:


res.head()


# In[13]:


def resample_hfq_multi(hfq_multi_daily,dates,period):
    hfq_multi = hfq_multi_daily.reindex(hfq_multi_daily.index.union([hfq_multi_daily.index[-1] + relativedelta(days=+1)])).resample(f'{period}T',label='right').ffill()
    hfq_multi = hfq_multi.loc[hfq_multi.index.normalize().isin(dates)]
    hfq_multi = pd.concat([hfq_multi.between_time('09:30','11:30',inclusive='right'),hfq_multi.between_time('13:00','15:00',inclusive='right')],axis=0).sort_index()
    hfq_multi.index.name='time'
    return hfq_multi


# In[14]:


print('resampling hfq_mutls')
hfq_multis = Parallel(n_jobs=4,verbose=5,backend='multiprocessing')(delayed(resample_hfq_multi)(hfq_multi_daily,[date],period) for period in [1,5,15,30])
hfq_multis.append(hfq_multi_daily)
hfq_multi_1min = hfq_multis[0]


# In[21]:


result_path = "./result"
def save_feature(feature,ff,hfq_multi,period):
    if feature == 'cjbs':
        if period != 'daily' and period != 1:
            ff = ff.resample(f'{period}T',label='right',closed='right').sum().reindex(hfq_multi.index)
            ff[np.isnan(hfq_multi)] = np.nan
            ff.to_csv(f'{result_path}/{period}min/{ff.index[-1].strftime("%Y%m%d")}/{feature}.csv.bz2',compression='bz2')
        elif period == 1:
            ff.to_csv(f'{result_path}/{period}min/{ff.index[-1].strftime("%Y%m%d")}/{feature}.csv.bz2',compression='bz2')
        else:
            ff = ff.resample(f'D').sum().reindex(hfq_multi.index)
            ff[np.isnan(hfq_multi)] = np.nan
            ff.to_csv(f'{result_path}/daily/{ff.index[-1].strftime("%Y%m%d")}/{feature}.csv.bz2',compression='bz2')
    elif feature == 'bcjbs':
        if period != 'daily' and period != 1:
            ff = ff.resample(f'{period}T',label='right',closed='right').sum().reindex(hfq_multi.index)
            ff[np.isnan(hfq_multi)] = np.nan
            ff.to_csv(f'{result_path}/{period}min/{ff.index[-1].strftime("%Y%m%d")}/{feature}.csv.bz2',compression='bz2')
        elif period == 1:
            ff.to_csv(f'{result_path}/{period}min/{ff.index[-1].strftime("%Y%m%d")}/{feature}.csv.bz2',compression='bz2')
        else:
            ff = ff.resample(f'D').sum().reindex(hfq_multi.index)
            ff[np.isnan(hfq_multi)] = np.nan
            ff.to_csv(f'{result_path}/daily/{ff.index[-1].strftime("%Y%m%d")}/{feature}.csv.bz2',compression='bz2')
    elif feature == 'scjbs':
        if period != 'daily' and period != 1:
            ff = ff.resample(f'{period}T',label='right',closed='right').sum().reindex(hfq_multi.index)
            ff[np.isnan(hfq_multi)] = np.nan
            ff.to_csv(f'{result_path}/{period}min/{ff.index[-1].strftime("%Y%m%d")}/{feature}.csv.bz2',compression='bz2')
        elif period == 1:
            ff.to_csv(f'{result_path}/{period}min/{ff.index[-1].strftime("%Y%m%d")}/{feature}.csv.bz2',compression='bz2')
        else:
            ff = ff.resample(f'D').sum().reindex(hfq_multi.index)
            ff[np.isnan(hfq_multi)] = np.nan
            ff.to_csv(f'{result_path}/daily/{ff.index[-1].strftime("%Y%m%d")}/{feature}.csv.bz2',compression='bz2')
    elif feature == 'volume_nfq':
        if period != 'daily' and period != 1:
            ff = ff.resample(f'{period}T',label='right',closed='right').sum().reindex(hfq_multi.index)
            ff[np.isnan(hfq_multi)] = np.nan
            ff.to_csv(f'{result_path}/{period}min/{ff.index[-1].strftime("%Y%m%d")}/{feature}.csv.bz2',compression='bz2')
            ff=(ff/hfq_multi)
            ff.to_csv(f'{result_path}/{period}min/{ff.index[-1].strftime("%Y%m%d")}/{feature[:-4]}.csv.bz2',compression='bz2')
        elif period == 1:
            ff.to_csv(f'{result_path}/{period}min/{ff.index[-1].strftime("%Y%m%d")}/{feature}.csv.bz2',compression='bz2')
            ff=(ff/hfq_multi)
            ff.to_csv(f'{result_path}/{period}min/{ff.index[-1].strftime("%Y%m%d")}/{feature[:-4]}.csv.bz2',compression='bz2')
        else:
            ff = ff.resample(f'D').sum().reindex(hfq_multi.index)
            ff[np.isnan(hfq_multi)] = np.nan
            ff.to_csv(f'{result_path}/daily/{ff.index[-1].strftime("%Y%m%d")}/{feature}.csv.bz2',compression='bz2')
            ff = (ff/hfq_multi)
            ff.to_csv(f'{result_path}/daily/{ff.index[-1].strftime("%Y%m%d")}/{feature[:-4]}.csv.bz2',compression='bz2')
    elif feature == 'bvolume_nfq':
        if period != 'daily' and period != 1:
            ff = ff.resample(f'{period}T',label='right',closed='right').sum().reindex(hfq_multi.index)
            ff[np.isnan(hfq_multi)] = np.nan
            ff.to_csv(f'{result_path}/{period}min/{ff.index[-1].strftime("%Y%m%d")}/{feature}.csv.bz2',compression='bz2')
            ff=(ff/hfq_multi)
            ff.to_csv(f'{result_path}/{period}min/{ff.index[-1].strftime("%Y%m%d")}/{feature[:-4]}.csv.bz2',compression='bz2')
        elif period == 1:
            ff.to_csv(f'{result_path}/{period}min/{ff.index[-1].strftime("%Y%m%d")}/{feature}.csv.bz2',compression='bz2')
            ff=(ff/hfq_multi)
            ff.to_csv(f'{result_path}/{period}min/{ff.index[-1].strftime("%Y%m%d")}/{feature[:-4]}.csv.bz2',compression='bz2')
        else:
            ff = ff.resample(f'D').sum().reindex(hfq_multi.index)
            ff[np.isnan(hfq_multi)] = np.nan
            ff.to_csv(f'{result_path}/daily/{ff.index[-1].strftime("%Y%m%d")}/{feature}.csv.bz2',compression='bz2')
            ff = (ff/hfq_multi)
            ff.to_csv(f'{result_path}/daily/{ff.index[-1].strftime("%Y%m%d")}/{feature[:-4]}.csv.bz2',compression='bz2')
    elif feature == 'svolume_nfq':
        if period != 'daily' and period != 1:
            ff = ff.resample(f'{period}T',label='right',closed='right').sum().reindex(hfq_multi.index)
            ff[np.isnan(hfq_multi)] = np.nan
            ff.to_csv(f'{result_path}/{period}min/{ff.index[-1].strftime("%Y%m%d")}/{feature}.csv.bz2',compression='bz2')
            ff=(ff/hfq_multi)
            ff.to_csv(f'{result_path}/{period}min/{ff.index[-1].strftime("%Y%m%d")}/{feature[:-4]}.csv.bz2',compression='bz2')
        elif period == 1:
            ff.to_csv(f'{result_path}/{period}min/{ff.index[-1].strftime("%Y%m%d")}/{feature}.csv.bz2',compression='bz2')
            ff=(ff/hfq_multi)
            ff.to_csv(f'{result_path}/{period}min/{ff.index[-1].strftime("%Y%m%d")}/{feature[:-4]}.csv.bz2',compression='bz2')
        else:
            ff = ff.resample(f'D').sum().reindex(hfq_multi.index)
            ff[np.isnan(hfq_multi)] = np.nan
            ff.to_csv(f'{result_path}/daily/{ff.index[-1].strftime("%Y%m%d")}/{feature}.csv.bz2',compression='bz2')
            ff = (ff/hfq_multi)
            ff.to_csv(f'{result_path}/daily/{ff.index[-1].strftime("%Y%m%d")}/{feature[:-4]}.csv.bz2',compression='bz2')
    elif feature == 'amount':
        if period != 'daily' and period != 1:
            ff = ff.resample(f'{period}T',label='right',closed='right').sum().reindex(hfq_multi.index)
            ff[np.isnan(hfq_multi)] = np.nan
            ff.to_csv(f'{result_path}/{period}min/{ff.index[-1].strftime("%Y%m%d")}/{feature}.csv.bz2',compression='bz2')
        elif period == 1:
            ff.to_csv(f'{result_path}/{period}min/{ff.index[-1].strftime("%Y%m%d")}/{feature}.csv.bz2',compression='bz2')
        else:
            ff = ff.resample(f'D').sum().reindex(hfq_multi.index)
            ff[np.isnan(hfq_multi)] = np.nan
            ff_old = pd.read_parquet(f'{result_path}/daily/{ff.index[-1].strftime("%Y%m%d")}/{feature}.csv.bz2',compression='bz2')
            ff_old = ff_old.loc[ff_old.index<ff.index[0]]
            ff = pd.concat([ff_old,ff],axis=0,copy=False)
            ff.to_csv(f'{result_path}/daily/{ff.index[-1].strftime("%Y%m%d")}/{feature}.csv.bz2',compression='bz2')
    elif feature == 'bamount':
        if period != 'daily' and period != 1:
            ff = ff.resample(f'{period}T',label='right',closed='right').sum().reindex(hfq_multi.index)
            ff[np.isnan(hfq_multi)] = np.nan
            ff.to_csv(f'{result_path}/{period}min/{ff.index[-1].strftime("%Y%m%d")}/{feature}.csv.bz2',compression='bz2')
        elif period == 1:
            ff.to_csv(f'{result_path}/{period}min/{ff.index[-1].strftime("%Y%m%d")}/{feature}.csv.bz2',compression='bz2')
        else:
            ff = ff.resample(f'D').sum().reindex(hfq_multi.index)
            ff[np.isnan(hfq_multi)] = np.nan
            ff_old = pd.read_parquet(f'{result_path}/daily/{ff.index[-1].strftime("%Y%m%d")}/{feature}.csv.bz2',compression='bz2')
            ff_old = ff_old.loc[ff_old.index<ff.index[0]]
            ff = pd.concat([ff_old,ff],axis=0,copy=False)
            ff.to_csv(f'{result_path}/daily/{ff.index[-1].strftime("%Y%m%d")}/{feature}.csv.bz2',compression='bz2')
    elif feature == 'samount':
        if period != 'daily' and period != 1:
            ff = ff.resample(f'{period}T',label='right',closed='right').sum().reindex(hfq_multi.index)
            ff[np.isnan(hfq_multi)] = np.nan
            ff.to_csv(f'{result_path}/{period}min/{ff.index[-1].strftime("%Y%m%d")}/{feature}.csv.bz2',compression='bz2')
        elif period == 1:
            ff.to_csv(f'{result_path}/{period}min/{ff.index[-1].strftime("%Y%m%d")}/{feature}.csv.bz2',compression='bz2')
        else:
            ff = ff.resample(f'D').sum().reindex(hfq_multi.index)
            ff[np.isnan(hfq_multi)] = np.nan
            ff_old = pd.read_parquet(f'{result_path}/daily/{ff.index[-1].strftime("%Y%m%d")}/{feature}.csv.bz2',compression='bz2')
            ff_old = ff_old.loc[ff_old.index<ff.index[0]]
            ff = pd.concat([ff_old,ff],axis=0,copy=False)
            ff.to_csv(f'{result_path}/daily/{ff.index[-1].strftime("%Y%m%d")}/{feature}.csv.bz2',compression='bz2')
    elif feature == 'closeprice_nfq':
        if period != 'daily' and period != 1:
            ff = ff.resample(f'{period}T',label='right',closed='right').last().reindex(hfq_multi.index)
            ff[np.isnan(hfq_multi)] = np.nan
            ff.to_csv(f'{result_path}/{period}min/{ff.index[-1].strftime("%Y%m%d")}/{feature}.csv.bz2',compression='bz2')
            ff=(ff*hfq_multi)
            ff.to_csv(f'{result_path}/{period}min/{ff.index[-1].strftime("%Y%m%d")}/{feature[:-4]}.csv.bz2',compression='bz2')
        elif period == 1:
            ff.to_csv(f'{result_path}/{period}min/{ff.index[-1].strftime("%Y%m%d")}/{feature}.csv.bz2',compression='bz2')
            ff=(ff*hfq_multi)
            ff.to_csv(f'{result_path}/{period}min/{ff.index[-1].strftime("%Y%m%d")}/{feature[:-4]}.csv.bz2',compression='bz2')
        else:
            ff = ff.resample(f'D').last().reindex(hfq_multi.index)
            ff[np.isnan(hfq_multi)] = np.nan
            ff.to_csv(f'{result_path}/daily/{ff.index[-1].strftime("%Y%m%d")}/{feature}.csv.bz2',compression='bz2')
            ff = (ff*hfq_multi)
            ff.to_csv(f'{result_path}/daily/{ff.index[-1].strftime("%Y%m%d")}/{feature[:-4]}.csv.bz2',compression='bz2')
    elif feature == 'openprice_nfq':
        if period != 'daily' and period != 1:
            ff = ff.resample(f'{period}T',label='right',closed='right').first().reindex(hfq_multi.index)
            ff[np.isnan(hfq_multi)] = np.nan
            ff.to_csv(f'{result_path}/{period}min/{ff.index[-1].strftime("%Y%m%d")}/{feature}.csv.bz2',compression='bz2')
            ff=(ff*hfq_multi)
            ff.to_csv(f'{result_path}/{period}min/{ff.index[-1].strftime("%Y%m%d")}/{feature[:-4]}.csv.bz2',compression='bz2')
        elif period == 1:
            ff.to_csv(f'{result_path}/{period}min/{ff.index[-1].strftime("%Y%m%d")}/{feature}.csv.bz2',compression='bz2')
            ff=(ff*hfq_multi)
            ff.to_csv(f'{result_path}/{period}min/{ff.index[-1].strftime("%Y%m%d")}/{feature[:-4]}.csv.bz2',compression='bz2')
        else:
            ff = ff.resample(f'D').first().reindex(hfq_multi.index)
            ff[np.isnan(hfq_multi)] = np.nan
            ff.to_csv(f'{result_path}/daily/{ff.index[-1].strftime("%Y%m%d")}/{feature}.csv.bz2',compression='bz2')
            ff = (ff*hfq_multi)
            ff.to_csv(f'{result_path}/daily/{ff.index[-1].strftime("%Y%m%d")}/{feature[:-4]}.csv.bz2',compression='bz2')
    elif feature == 'highprice_nfq':
        if period != 'daily' and period != 1:
            ff = ff.resample(f'{period}T',label='right',closed='right').max().reindex(hfq_multi.index)
            ff[np.isnan(hfq_multi)] = np.nan
            ff.to_csv(f'{result_path}/{period}min/{ff.index[-1].strftime("%Y%m%d")}/{feature}.csv.bz2',compression='bz2')
            ff=(ff*hfq_multi)
            ff.to_csv(f'{result_path}/{period}min/{ff.index[-1].strftime("%Y%m%d")}/{feature[:-4]}.csv.bz2',compression='bz2')
        elif period == 1:
            ff.to_csv(f'{result_path}/{period}min/{ff.index[-1].strftime("%Y%m%d")}/{feature}.csv.bz2',compression='bz2')
            ff=(ff*hfq_multi)
            ff.to_csv(f'{result_path}/{period}min/{ff.index[-1].strftime("%Y%m%d")}/{feature[:-4]}.csv.bz2',compression='bz2')
        else:
            ff = ff.resample(f'D').max().reindex(hfq_multi.index)
            ff[np.isnan(hfq_multi)] = np.nan
            ff.to_csv(f'{result_path}/daily/{ff.index[-1].strftime("%Y%m%d")}/{feature}.csv.bz2',compression='bz2')
            ff = (ff*hfq_multi)
            ff.to_csv(f'{result_path}/daily/{ff.index[-1].strftime("%Y%m%d")}/{feature[:-4]}.csv.bz2',compression='bz2')
    elif feature == 'lowprice_nfq':
        if period != 'daily' and period != 1:
            ff = ff.resample(f'{period}T',label='right',closed='right').min().reindex(hfq_multi.index)
            ff[np.isnan(hfq_multi)] = np.nan
            ff.to_csv(f'{result_path}/{period}min/{ff.index[-1].strftime("%Y%m%d")}/{feature}.csv.bz2',compression='bz2')
            ff=(ff*hfq_multi)
            ff.to_csv(f'{result_path}/{period}min/{ff.index[-1].strftime("%Y%m%d")}/{feature[:-4]}.csv.bz2',compression='bz2')
        elif period == 1:
            ff.to_csv(f'{result_path}/{period}min/{ff.index[-1].strftime("%Y%m%d")}/{feature}.csv.bz2',compression='bz2')
            ff=(ff*hfq_multi)
            ff.to_csv(f'{result_path}/{period}min/{ff.index[-1].strftime("%Y%m%d")}/{feature[:-4]}.csv.bz2',compression='bz2')
        else:
            ff = ff.resample(f'D').min().reindex(hfq_multi.index)
            ff[np.isnan(hfq_multi)] = np.nan
            ff.to_csv(f'{result_path}/daily/{ff.index[-1].strftime("%Y%m%d")}/{feature}.csv.bz2',compression='bz2')
            ff = (ff*hfq_multi)
            ff.to_csv(f'{result_path}/daily/{ff.index[-1].strftime("%Y%m%d")}/{feature[:-4]}.csv.bz2',compression='bz2')


# In[26]:


print('saving hfq_mutls')
for i,period in enumerate(['1min','5min','15min','30min','daily']):
    if not os.path.exists(f'{result_path}/{period}/{date.strftime("%Y%m%d")}/'):
        os.makedirs(f'{result_path}/{period}/{date.strftime("%Y%m%d")}/')
        print(f'{result_path}/{period}min/{date.strftime("%Y%m%d")}/')
    hfq_multis[i].to_csv(f'{result_path}/{period}/{date.strftime("%Y%m%d")}/hfq_multi.csv.bz2',compression='bz2')


# In[27]:


def pivoting(feature,df,hfq_multi_1min):
    tmp = df.pivot(index='datetime',columns='securityid',values=feature)
    ff = pd.concat([tmp.between_time('09:30','11:30',inclusive='right'),tmp.between_time('13:00','15:00',inclusive='right')],axis=0).sort_index()
    ff = ff.reindex(hfq_multi_1min.index,columns=hfq_multi_1min.columns)
    if feature in ['cjbs','bcjbs','scjbs','volume_nfq','bvolume_nfq','svolume_nfq','amount','bamount','samount']:
        ff = ff.fillna(0)
    elif feature == 'closeprice_nfq':
        ff = ff.fillna(method='ffill')
    ff[np.isnan(hfq_multi_1min)] = np.nan
    ff.index.name = 'time'
    return ff


# In[28]:


def calc_vwap(amount,volume_nfq,hfq_multi,period,name):
    fns = []
    if period != 'daily':
        amount = amount.resample(f'{period}T',label='right',closed='right').sum().reindex(hfq_multi.index)
        volume_nfq = volume_nfq.resample(f'{period}T',label='right',closed='right').sum().reindex(hfq_multi.index)
        ff = (amount/volume_nfq).replace([np.inf,-np.inf],np.nan)
        ff[np.isnan(hfq_multi)] = np.nan
        ff.index.name='time'
        ff.to_csv(f'{result_path}/{period}min/{ff.index[-1].strftime("%Y%m%d")}/{name}_nfq.csv.bz2',compression='bz2')
        fns.append(ff)
        volume = volume_nfq/hfq_multi
        ff = (amount/volume).replace([np.inf,-np.inf],np.nan)
        ff[np.isnan(hfq_multi)] = np.nan
        ff.index.name='time'
        ff.to_csv(f'{result_path}/{period}min/{ff.index[-1].strftime("%Y%m%d")}/{name}.csv.bz2',compression='bz2')
        fns.append(ff)
    else:
        amount = amount.resample(f'D').sum().reindex(hfq_multi.index)
        volume_nfq = volume_nfq.resample(f'D').sum().reindex(hfq_multi.index)
        ff = (amount/volume_nfq).replace([np.inf,-np.inf],np.nan)
        ff[np.isnan(hfq_multi)] = np.nan
        ff.index.name='time'
        ff.to_csv(f'{result_path}/daily/{ff.index[-1].strftime("%Y%m%d")}/{name}_nfq.csv.bz2',compression='bz2')
        fns.append(ff)
        volume = volume_nfq/hfq_multi
        ff = (amount/volume).replace([np.inf,-np.inf],np.nan)
        ff[np.isnan(hfq_multi)] = np.nan
        ff.index.name='time'
        ff.to_csv(f'{result_path}/daily/{ff.index[-1].strftime("%Y%m%d")}/{name}.csv.bz2',compression='bz2')
        fns.append(ff)
    return fns


# In[29]:


feature_cols = ['cjbs','bcjbs','scjbs','volume_nfq','bvolume_nfq','svolume_nfq','amount','bamount','samount','closeprice_nfq','openprice_nfq','highprice_nfq','lowprice_nfq']
print('transforming features')
features = Parallel(n_jobs=4,verbose=5,backend='multiprocessing')(delayed(pivoting)(feature,res[['datetime','securityid',feature]],hfq_multi_1min) for i,feature in enumerate(feature_cols))


# In[33]:


print('calculating vwaps')
final = Parallel(n_jobs=16,verbose=5,backend='multiprocessing')(delayed(calc_vwap)(a,b,hfq_multi,period,name) for a,b,name in zip([features[6],features[7],features[8]],[features[3],features[4],features[5]],['vwap','bvwap','svwap']) for period,hfq_multi in zip([1,5,15,30,'daily'],hfq_multis))

pr.disable()
s = io.StringIO()
sortby = "cumtime"  # 仅适用于 3.6, 3.7 把这里改成常量了
ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
ps.print_stats()
pr.dump_stats("pipeline.prof")
print(s.getvalue())
# 

# hfq_multi_daily
# col:行情数据文件ID
# row:日期
# dates 选择一批日期
# period 采样精度
# 
# [1]在原来日期上增加一天

# hfq_multi_daily.shape
