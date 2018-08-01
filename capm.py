#CAPM Analysis Python Code

from mpi4py import MPI #MPI package for cluster analysis
import pandas as pd 
import datetime 
import numpy as np
import statsmodels.api as sm
import csv
import os
import zipfile #read the csv files directly

CONST_INTERVAL=300 #interval in seconds
CONST_BEGINTIME='9:30:00'
CONST_ENDTIME='16:00:00' 

#get the mpi object 
comm=MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

def expand_gap(x):
	#function to expand gaps 
	max_lim=x['genjud_incre'].iloc[0]+1
	incre_list=list(range(0,max_lim))
	incre_arr=np.array(incre_list).reshape(max_lim, 1)
	tmpdf=pd.DataFrame(incre_arr, columns=["increment"])
	res_df=x.merge(tmpdf,on="increment",how="outer")
	res_df=res_df.sort_values(['increment']) #sort before ffill
	res_df=res_df.fillna(method='ffill').fillna(method='bfill')
	return res_df

def calculate_return(x):
	#function used to calculate the returns
	x['returns']=(x['MIDPRICE']-x['MIDPRICE'].shift(1))/x['MIDPRICE'].shift(1)
	return x

def capm_analysis(name):

	begin=str(datetime.datetime.now())
	name_date=filter(str.isdigit, name) #the date indicated on the trade and quote files
	#read the zip file and convert it to csv
	with zipfile.ZipFile(name) as zip:
		csv_name=name.replace("zip","csv")
		with zip.open(csv_name) as csv_file:
			df=pd.read_csv(csv_file)
	df['TIME']=df['DATE'].astype(str)+' '+df['TIME']
	df['genesis']=df['DATE'].astype(str) + ' ' + CONST_BEGINTIME #begin time
	df['judgement']=df['DATE'].astype(str) + ' ' + CONST_ENDTIME  #end time
	df['TIME']=pd.to_datetime(df['TIME'],format='%Y%m%d %H:%M:%S')
	df['genesis']=pd.to_datetime(df['genesis'],format='%Y%m%d %H:%M:%S')
	df['judgement']=pd.to_datetime(df['judgement'],format='%Y%m%d %H:%M:%S')

	#select certain columns that I want 
	df=df[['TIME','genesis','SYMBOL','BB','BO','judgement','DATE']]

	#select the data of certain time range 
	begin_time=datetime.datetime.strptime(CONST_BEGINTIME,'%H:%M:%S').time()
	end_time=datetime.datetime.strptime(CONST_ENDTIME,'%H:%M:%S').time()
	mask=(df['TIME'].dt.time>=begin_time)&(df['TIME'].dt.time<=end_time)
	df=df.loc[mask]

	#calculate the mid price 
	df['MIDPRICE']=(df['BB']+df['BO'])/2
	#If the first quote has only a bid or an ask, then the mid=max(bid, ask)
	df.ix[df["BB"]==0, df.columns.get_loc('MIDPRICE')]=df.ix[df["BB"]==0, df.columns.get_loc('BO')]
	df.ix[df["BO"]==0, df.columns.get_loc('MIDPRICE')]=df.ix[df["BO"]==0, df.columns.get_loc('BB')]

	#filter the column for bid-ask spreads exceeds $10 and 1000 bps
	#df['bps']=(df['BO']-df['BB'])/df['MIDPRICE']
	#df['spread']=df['BO']-df['BB']
	#df=df[((df['bps']<0.1) | (df['spread']<10))]

	df['diff']=df['TIME']-df['genesis'] #get the time difference from the beginning
	df['diff_sec']=df['diff'].dt.seconds 
	df['increment']=np.floor(df['diff_sec']/CONST_INTERVAL).astype(int) #the time incremental in CONST_INTERVAL
	df=df.groupby(['SYMBOL','DATE','increment']).tail(1).reset_index(drop=True) #only keep the last observation per interval. forward fill if the value is missing

	#calculate the maximum increment value
	df['gen_jud_diff'] = df['judgement']-df['genesis']
	df['gen_jud_diff_sec']=df['gen_jud_diff'].dt.seconds
	df['genjud_incre']=np.floor(df['gen_jud_diff_sec']/CONST_INTERVAL).astype(int)

	df=df[['TIME','SYMBOL','increment','genjud_incre','DATE','MIDPRICE']]

	#now expand any gaps in between
	df=df.groupby(['SYMBOL','DATE']).apply(expand_gap)
	df = df.reset_index(drop=True)
	df=df.sort_values(['SYMBOL','DATE','increment'])
	df = df.reset_index(drop=True)

	#re-select columns 
	df=df[['SYMBOL','increment','MIDPRICE','DATE']]

	#calculate the returns by ticker
	df=df.groupby(['SYMBOL','DATE']).apply(calculate_return)

	#reduce the number of varaibles for PCA
	df=df[['SYMBOL','increment','returns']]
	df = df[np.isfinite(df['returns'])]
	df = df.reset_index(drop=True)

	#reshape for CAPM
	df=df.pivot(index='increment',columns='SYMBOL',values='returns')
	resList=[] #the list that holds all the results 
	variableRow=['Date','Ticker','Coeff_const','StdErr_const','Tstats_const','Coeff_ticker','StdErr_ticker','Tstats_ticker','Rsq','Adj_Rsq']
	resList.append(variableRow)
	
	#start CAPM regression for each column 
	for column in df: 
		Y,X = df[column], df['SPY']
		X = sm.add_constant(X)
		result=sm.OLS(Y,X).fit()
		std=result.bse #the standard devitations 
		tstats=result.tvalues #the t-values
		rsq_adj=result.rsquared_adj #adjusted r squared
		rsq=result.rsquared #r squared
		coefs=result.params #the coefficients 
		ticker=column
		#now formulate the results
		current_res=[name_date,column,coefs[0],std[0],tstats[0],coefs[1],std[1],tstats[1],rsq,rsq_adj]
		resList.append(current_res)

	with open(name_date+"_reg.csv","wb") as f:
		writer = csv.writer(f)
		writer.writerows(resList)

	end=str(datetime.datetime.now())
	lengths=[]
	lengths.append(begin)
	lengths.append(end)

	with open(name_date+"time.txt",'w') as file:
		for l in lengths:
			file.write(l+"\n")

if rank == 0:
	tmparr=os.listdir("./")
	arr=[]
	for t in tmparr:
		if t[0:4]=='NBBO' and t[-3:]=='zip':
			#confirms that it is the file we want to analyse 
			arr.append(t)
	chunks=[[] for _ in range(size)]
	for i, chunk in enumerate(arr):
		chunks[i % size].append(chunk)
else:
	data = None
	chunks=None
data = comm.scatter(chunks, root=0)
for d in data: 
	capm_analysis(d)