import pandas as pd

df=pd.read_csv("NBBOM_20170103nbbo.csv",chunksize=100000)

with open ("f.txt",'w') as file:
	for line in df:
		b=line["QU_CANCEL"].notnull().values.any()
		file.write(str(b)+"\n")

df=pd.read_csv("NBBOM_20170103nbbo.csv",chunksize=100000)

with open ("b.txt",'w') as file:
	for line in df:
		b=line['BEST_BIDSIZ'].eq(0).any()
		file.write(str(b)+"\n")

df=pd.read_csv("NBBOM_20170103nbbo.csv",chunksize=100000)

with open ("a.txt",'w') as file:
	for line in df:
		b=line['BEST_ASKSIZ'].eq(0).any()
		file.write(str(b)+"\n")

df=pd.read_csv("NBBOM_20170103nbbo.csv",chunksize=200000)
acount=0
for line in df:
	line[line['BEST_ASKSIZ'].eq(0)].to_csv(str(acount)+"a.csv")
	acount=acount+1

df=pd.read_csv("NBBOM_20170103nbbo.csv",chunksize=200000)
bcount=0
for line in df:
	line[line['BEST_BIDSIZ'].eq(0)].to_csv(str(bcount)+"b.csv")
	bcount=bcount+1