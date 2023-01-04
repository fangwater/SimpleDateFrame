loc[aa.trap == "s"]
aa = aa.loc[aa.trap>0]

[] 内部是表达式　

返回一个place_holder

由Frame的工厂类生成，保存列表的shared_ptr

Hash[]

类似于copy on write的手法　在需要数据之前不产生新的表


aa[place_holder] = aa

这也是为了防止
aa[equation_1 && equation_2 || equation_3]这种情况

因此place_holder需要支持基本计算包括&& || 


aa[place_holder] -> new_DataFrame



equation

aa.tradp>0

//select column
aa.col<T>("tradp") ---> column<T> 

aa.col<T>("tradp") > 0 ---> place_holder

aa[place_holder] = ---> DataFrame

//ok
aa.col<T>('tradamt') = aa.col<T>('tradv') * aa.col<T>('tradp')

//
aa_B = aa[aa.col<std::string>("bs") == 'B']
aa_S = aa[aa.col<std::string>("bs") == 'S']

//groupBy
res = aa.groupby([pd.Grouper(key='datetime',freq=f'T',closed='right',label='right',dropna=False),'securityid'])['tradp'].count().to_frame()


Define a Grouper?
Most use group by columns

Grouper for columns by value
partial by value --> multi_hash -->place_holder

Grouper for columns by date_range
 
Grouper for columns by time_freq

The interface of apply a Group

value
pd.Grouper(key="tp",freq=f"12M")

do the s

reset_index

aa.col[""]
key = 'datetime' --> aa.col<absl::date>("datetime")
freq = f'T'
col<T>.resample(f'T') -->place_holder

reset_index()

count()

sum()

max()/min()/last()/first()

concat()



