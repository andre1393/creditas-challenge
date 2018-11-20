import sys
import numpy as np
import pandas as pd
import datetime as dt
import time

from SourceReader import SourceReader
from Pipeline import Pipeline
import threading

def main(args):
	host = 'localhost'
	dbname = 'creditas'
	user = 'postgres'
	password = 'andre4918'
	conn_string = 'host={} dbname={} user={} password={}'.format(host, dbname, user, password)
	dataset = '../../datasets/DCA_Dataset_Loan_Transactions.csv'
	chunksize = 10000

	t0 = time.time()

	# carregando o dataset
	reader = SourceReader(dataset, chunksize = 10000, names = ['guarantee_number', 'transaction_report_id', 'amount_usd', 'currency_name', 'end_date', 'business_sector', 'city_town', 'state_province_region_name', 'State_Province_Region_code', 'country_name', 'region_name', 'latitude', 'longitude', 'is_woman_owned', 'is_first_time_borrower', 'business_size'])
	df_chunk = reader.read()
	
	thread = 0
	pipelines = {}
	for df in df_chunk:
		pipelines[thread] = Pipeline(str(thread), dbname, "guarantee_number, transaction_report_id, amount_usd, currency_name, end_date, business_sector, city_town, state_province_region_name, State_Province_Region_code, country_name, region_name, latitude, longitude, is_woman_owned, is_first_time_borrower, business_size", conn_string, df)
		pipelines[thread].start()
		thread += 1

	t1 = time.time()
	print('todo o dataset ja teve seu processamento iniciado em %.2f s' % (t1 - t0))

	while(threading.active_count() > 1):
		count = threading.active_count()
		print('%d threads em execucao' % count)
		time.sleep(5)

	print('tempo total de processamento: %.2f s' % (time.time() - t1))

if __name__ == "__main__":
	main(sys.argv[1:])