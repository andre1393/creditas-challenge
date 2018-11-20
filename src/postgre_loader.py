import sys
import psycopg2
import numpy as np
import pandas as pd
import datetime as dt
import time

def main(args):
	host = 'localhost'
	dbname = 'creditas'
	user = 'postgres'
	password = 'andre4918'
	conn_string = 'host={} dbname={} user={} password={}'.format(host, dbname, user, password)

	begin_time = time.time()
	conn = psycopg2.connect(conn_string)
	print('conexao com o banco {} estabelecida com sucesso'.format(dbname))
	cur = conn.cursor()
	psycopg2.extensions.register_adapter(float, nan_to_null)
	print('carregando dataset')
	df = pd.read_csv('../../datasets/DCA_Dataset_Loan_Transactions.csv', names = ['guarantee_number', 'transaction_report_id', 'amount_usd', 'currency_name', 'end_date', 'business_sector', 'city_town', 'state_province_region_name', 'State_Province_Region_code', 'country_name', 'region_name', 'latitude', 'longitude', 'is_woman_owned', 'is_first_time_borrower', 'business_size'], header = 0)
	#for df in df_chunk:
	if True:
		print('dataset carregado')
		df['end_date'] = df['end_date'].apply(lambda x: dt.datetime.strptime(x, '%m/%d/%Y'))
		#df['business_sector'] = df['business_sector'].fillna('')
		df['is_woman_owned'] = df['is_woman_owned'].apply(toBit)
		df['is_first_time_borrower'] = df['is_first_time_borrower'].apply(toBit)
		df = df.replace("'", "''", regex = True)
		#df = df.fillna(0)
		lines_processed = 0
		lines_saved = 0
		chunk_init = time.time()
		params = ','.join("('{}','{}',{},'{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')".format(*x) for idx, x in df.iterrows())
		cur.execute('INSERT INTO transactions (guarantee_number, transaction_report_id, amount_usd, currency_name, end_date, business_sector, city_town, state_province_region_name, State_Province_Region_code, country_name, region_name, latitude, longitude, is_woman_owned, is_first_time_borrower, business_size) values %s' % params)
		conn.commit()
		elapsed_time = time.time() - chunk_init
		print('{} registros processados em {}'.format(1000, elapsed_time))

	print('tempo total de processamento: {}'.format(time.time() - begin_time))
	'''	for idx, item in df.iterrows():
			params = ('transactions',) + tuple(item)
			try:
				cur.execute('INSERT INTO %s (guarantee_number, transaction_report_id, amount_usd, currency_name, end_date, business_sector, city_town, state_province_region_name, State_Province_Region_code, country_name, region_name, latitude, longitude, is_woman_owned, is_first_time_borrower, business_size) values(\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\')' % params)
				lines_processed += 1
				lines_saved += 1
			except:
				lines_processed += 1
				print('Falha ao executar o registro {}'.format(lines_processed))
				print(item)
				continue
		conn.commit()
		elapsed_time = time.time() - begin_time
		print('{} registros processados em {}'.format(lines_processed, elapsed_time))'''
def toBit(x):
	return 1 if x == 't' else 0

def nan_to_null(f, _NULL=psycopg2.extensions.AsIs('NULL'), _NaN=np.NaN, _Float=psycopg2.extensions.Float):
    if f is not _NaN:
        return _Float(f)
    return _NULL

if __name__ == "__main__":
	main(sys.argv[1:])