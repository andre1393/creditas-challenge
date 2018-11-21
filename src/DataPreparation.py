import numpy as np
import pandas as pd
import datetime as dt
import time

class DataPreparation:

	def __init__(self, thread_name, preprocess = '{}', transform_columns = {}, hide_apostrophe = True):
		self.thread_name = thread_name
		self.transform_columns = transform_columns
		self.hide_apostrophe = hide_apostrophe
		self.preprocess = preprocess

	def preprocess_loan_transactions(self, _df):
		df = _df.copy()
		t0 = time.time()
		df['end_date'] = df['end_date'].apply(lambda x: dt.datetime.strptime(x, '%m/%d/%Y'))
		df['is_woman_owned'] = df['is_woman_owned'].apply(self.toBit)
		df['is_first_time_borrower'] = df['is_first_time_borrower'].apply(self.toBit)
		
		# inclui troca textos que tem (') por ('') para evitar erros na hora de salvar no banco
		df = df.replace("'", "''", regex = True)
		t1 = time.time()
		print('thread %s - preparacao dos dados executada em %.2f s' % (self.thread_name, t1 - t0))

		return df

	def preprocess(self, _df):
		date_columns = self.preprocess.get('date_columns') if self.preprocess.get('date_columns') != None else []
		date_format = self.preprocess.get('date_format') 
		int_columns = self.preprocess.get('int_columns') if self.preprocess.get('int_columns') != None else []
		money_columns = self.preprocess.get('money_columns') if self.preprocess.get('money_columns') != None else []
		replace_apostrophe = self.preprocess.get('replace_apostrophe') if self.preprocess.get('replace_apostrophe') else True

		df = _df.copy()
		t0 = time.time()

		if date_format != None:
			for column in date_columns:
				print(column)
				df[column] = df[column].apply(lambda x: dt.datetime.strptime(x, date_format))

		for int_column in int_columns:
			df[int_column] = df[int_column].apply(lambda x: np.nan if x is np.nan else x.replace(',',''))

		for money_column in money_columns:
			df[money_column] = df[money_column].apply(lambda x: float(str(x).replace('$', '').replace(',', '')))

		if replace_apostrophe:
			df = df.replace("'", "''", regex = True)
		t1 = time.time()
		print('thread %s - preparacao dos dados executada em %.2f s' % (self.thread_name, t1 - t0))

		return df
	def toBit(self, x):
		return 1 if x == 't' else 0