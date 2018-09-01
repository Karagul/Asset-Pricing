import wrds
import pandas as pd

def nbbo(table_name):
	'''
	The function to calculate NBBO using TAQ data on NASDAQ
	'''
	data = db.get_table(library='taqmsec', table=table_name, columns=['date','time_m','ex','sym_root','sym_suffix','bid','ask','qu_cond','natbbo_ind','nasdbbo_ind'])

year='201501'
db = wrds.Connection()
#list of all tables
table_list=db.list_tables(library="taqmsec")
#list of tables containing the taq data
taq_list = [i for i in table_list if i.startswith('cqm_'+year)]

for tb in taq_list: 
	nbbo(tb)