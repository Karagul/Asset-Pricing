#PCA Analysis Python Code

from mpi4py import MPI #MPI package for cluster analysis
import pandas as pd 
import datetime 
import numpy as np
from sklearn.decomposition import PCA #PCA Package
import os

CONST_INTERVAL=5 #interval in seconds
CONST_BEGINTIME='9:30:00.000000'
CONST_ENDTIME='16:00:00.000000' 

#get the mpi object 
comm=MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

def group_increment_to_end(x):
	#applied to group by function to increment to the end
	first_row=x.iloc[0:1]
	last_row=x.iloc[-1:]
	if first_row['increment'].iloc[0]!=0:
		first_increment=first_row['increment'].iloc[0]
		while (first_increment>0):
			new_row=first_row.copy()
			first_increment=first_increment-1
			new_row['increment'].iloc[0]=first_increment
			x=x.append(new_row)
	if last_row['increment'].iloc[0]<last_row['genjud_incre'].iloc[0]:
		max_increment=last_row['genjud_incre'].iloc[0]
		last_increment=last_row['increment'].iloc[0]
		while(last_increment<max_increment):
			new_row=last_row.copy()
			last_increment=last_increment+1
			new_row['increment'].iloc[0]=last_increment
			x=x.append(new_row)
	return x

def expand_gap(x):
	#function to expand gaps 
	#iterate through to find gaps
	x['tmp_diff']=x['increment'].shift(-1)-x['increment']
	tmp_df=x[x['tmp_diff']>1]
	for i in range (0, len(tmp_df)):
		expand_incre = tmp_df.iloc[i,tmp_df.columns.get_loc('increment')]
		target_incre = tmp_df.iloc[i,tmp_df.columns.get_loc('increment')]+tmp_df.iloc[i,tmp_df.columns.get_loc('tmp_diff')]
		while (target_incre-expand_incre)>1:
			tmp_row=tmp_df.iloc[i:i+1]
			new_row=tmp_row.copy()
			expand_incre=expand_incre+1
			new_row['increment'].iloc[0]=expand_incre
			x=x.append(new_row)
	return x

def calculate_return(x):
	#function used to calculate the returns
	x['returns']=(x['MIDPRICE']-x['MIDPRICE'].shift(1))/x['MIDPRICE'].shift(1)
	return x

def pca_analysis(name):

	begin=str(datetime.datetime.now())
	name_date=filter(str.isdigit, name) #the date indicated on the trade and quote files
	df=pd.read_csv(name)

	df['TIME_M']=df['DATE'].astype(str)+' '+df['TIME_M']
	df['genesis']=df['DATE'].astype(str) + ' ' + CONST_BEGINTIME #begin time
	df['judgement']=df['DATE'].astype(str) + ' ' + CONST_ENDTIME  #end time
	df['TIME_M']=pd.to_datetime(df['TIME_M'],format='%Y%m%d %H:%M:%S.%f')
	df['genesis']=pd.to_datetime(df['genesis'],format='%Y%m%d %H:%M:%S.%f')
	df['judgement']=pd.to_datetime(df['judgement'],format='%Y%m%d %H:%M:%S.%f')

	#select certain columns that I want 
	df=df[['TIME_M','genesis','SYM_ROOT','BEST_BID','BEST_ASK','judgement','DATE']]

	#select the data of certain time range 
	begin_time=datetime.datetime.strptime(CONST_BEGINTIME,'%H:%M:%S.%f').time()
	end_time=datetime.datetime.strptime(CONST_ENDTIME,'%H:%M:%S.%f').time()
	mask=(df['TIME_M'].dt.time>=begin_time)&(df['TIME_M'].dt.time<=end_time)
	df=df.loc[mask]

	#we smooth the quotes by second first, to make sure that when we use increment in unit of second, the noise of quotes will not comeinto play
	df['TIME_S']=df.TIME_M.map(lambda t:t.strftime('%Y%m%d %H:%M:%S'))
	df=df.groupby(['SYM_ROOT','DATE','TIME_S']).tail(1).reset_index(drop=True) #only keep the last observation per interval. forward fill if the value is missing	

	#calculate the mid price 
	df['MIDPRICE']=(df['BEST_BID']+df['BEST_ASK'])/2
	#If the first quote has only a bid or an ask, then the mid=max(bid, ask)
	df.ix[df["BEST_BID"]==0, df.columns.get_loc('MIDPRICE')]=df.ix[df["BEST_BID"]==0, df.columns.get_loc('BEST_ASK')]
	df.ix[df["BEST_ASK"]==0, df.columns.get_loc('MIDPRICE')]=df.ix[df["BEST_ASK"]==0, df.columns.get_loc('BEST_BID')]

	#filter the column for bid-ask spreads exceeds $10 and 1000 bps
	df['bps']=(df['BEST_ASK']-df['BEST_BID'])/df['MIDPRICE']
	df['spread']=df['BEST_ASK']-df['BEST_BID']
	df=df[((df['bps']<0.1) | (df['spread']<10))]

	df['diff']=df['TIME_M']-df['genesis'] #get the time difference from the beginning
	df['diff_sec']=df['diff'].dt.seconds 
	df['increment']=np.floor(df['diff_sec']/CONST_INTERVAL).astype(int) #the time incremental in CONST_INTERVAL
	df=df.groupby(['SYM_ROOT','DATE','increment']).tail(1).reset_index() #only keep the last observation per interval. forward fill if the value is missing

	#now expand any gaps in between
	df=df.groupby(['SYM_ROOT','DATE']).apply(expand_gap)
	df = df.reset_index(drop=True)
	df=df.sort_values(['SYM_ROOT','DATE','increment'])
	df = df.reset_index(drop=True)

	df['gen_jud_diff'] = df['judgement']-df['genesis']
	df['gen_jud_diff_sec']=df['gen_jud_diff'].dt.seconds
	df['genjud_incre']=np.floor(df['gen_jud_diff_sec']/CONST_INTERVAL).astype(int)

	#re-select columns 
	df=df[['TIME_M','SYM_ROOT','increment','genjud_incre','DATE','MIDPRICE']]

	df=df.groupby(['SYM_ROOT','DATE']).apply(group_increment_to_end)

	#reset the index
	df = df.reset_index(drop=True)

	#re-select columns 
	df=df[['SYM_ROOT','increment','MIDPRICE','DATE']]

	#re-sort the columns
	df=df.sort_values(['SYM_ROOT','DATE','increment'])

	#calculate the returns by ticker
	df=df.groupby(['SYM_ROOT','DATE']).apply(calculate_return)

	#reduce the number of varaibles for PCA
	df=df[['SYM_ROOT','increment','returns']]
	df = df[np.isfinite(df['returns'])]
	df = df.reset_index(drop=True)

	#reshape for PCA
	df=df.pivot(index='increment',columns='SYM_ROOT',values='returns')
	df_list=df.values.tolist()

	#start pca
	pca=PCA()
	res=pca.fit(df_list)

	eigenvalues=pca.explained_variance_
	eigenvalues_percent=pca.explained_variance_ratio_
	eigenvectors=pca.components_ 

	eigendf=pd.DataFrame(eigenvectors)
	eigendf.to_csv(name_date+"eigenvector.csv")
	eigenvalues.tofile(name_date+"eigenvalues.csv",sep='\n')
	eigenvalues_percent.tofile(name_date+"eigenvalues_percent.csv",sep='\n')

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
		if t[0:4]=='NBBO' and t[-3:]=='csv':
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
	pca_analysis(d)