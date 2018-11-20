import sys
import psycopg2
import numpy as np
import pandas as pd
import datetime as dt
import time

from SourceReader import SourceReader
from PostgreLoader import PostgreLoader
from DataPreparation import DataPreparation

def main(args):
	host = 'localhost'
	dbname = 'creditas'
	user = 'postgres'
	password = 'andre4918'
	conn_string = 'host={} dbname={} user={} password={}'.format(host, dbname, user, password)
	dataset = '../../datasets/DCA_Dataset_Loan_Transactions - Copia.csv'
	chunksize = 10000

	# carregando o dataset
	reader = SourceReader(dataset, chunksize = 10000, names = ['guarantee_number', 'transaction_report_id', 'amount_usd', 'currency_name', 'end_date', 'business_sector', 'city_town', 'state_province_region_name', 'State_Province_Region_code', 'country_name', 'region_name', 'latitude', 'longitude', 'is_woman_owned', 'is_first_time_borrower', 'business_size'])
	df_chunk = reader.read()
	
	loader = PostgreLoader(dbname, "guarantee_number, transaction_report_id, amount_usd, currency_name, end_date, business_sector, city_town, state_province_region_name, State_Province_Region_code, country_name, region_name, latitude, longitude, is_woman_owned, is_first_time_borrower, business_size", conn_string)
	preprocess = DataPreparation()

	for df in df_chunk:
		df = preprocess.process(df)
		loader.load(df)

	print('tempo total de processamento: %.2f s' % (time.time() - t2))

if __name__ == "__main__":
	main(sys.argv[1:])