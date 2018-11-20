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
	dataset = '../../datasets/DCA_Dataset_Loan_Transactions.csv'

	t0 = time.time()
	conn = psycopg2.connect(conn_string)
	print('conexao com o banco %s estabelecida com sucesso em %.2f s' % (dbname, time.time() - t0))
	cur = conn.cursor()
	print('carregando dataset %s' % dataset)
	t1 = time.time()

	# carregando o dataset
	df = pd.read_csv(dataset, names = ['guarantee_number', 'transaction_report_id', 'amount_usd', 'currency_name', 'end_date', 'business_sector', 'city_town', 'state_province_region_name', 'State_Province_Region_code', 'country_name', 'region_name', 'latitude', 'longitude', 'is_woman_owned', 'is_first_time_borrower', 'business_size'], header = 0)
	print('dataset carregado em %.2f s' % (time.time() - t1))
	
	t2 = time.time()
	#for df in df_chunk:
	if True:
		t3 = time.time()
		# data preparation
		df['end_date'] = df['end_date'].apply(lambda x: dt.datetime.strptime(x, '%m/%d/%Y'))
		df['is_woman_owned'] = df['is_woman_owned'].apply(toBit)
		df['is_first_time_borrower'] = df['is_first_time_borrower'].apply(toBit)
		df = df.replace("'", "''", regex = True)
		t4 = time.time()
		print('preparacao dos dados executada em %.2f s' % (t4 - t3))

		params = ','.join("('{}','{}',{},'{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')".format(*x) for idx, x in df.iterrows()).replace("'nan'", 'NULL')
		cur.execute('INSERT INTO transactions (guarantee_number, transaction_report_id, amount_usd, currency_name, end_date, business_sector, city_town, state_province_region_name, State_Province_Region_code, country_name, region_name, latitude, longitude, is_woman_owned, is_first_time_borrower, business_size) values %s' % params)
		conn.commit()
		t5 = time.time()
		print('%d registros processados em %.2f s' % (len(df), t5 - t4))

	print('tempo total de processamento: %.2f s' % (time.time() - t2))

def toBit(x):
	return 1 if x == 't' else 0

if __name__ == "__main__":
	main(sys.argv[1:])