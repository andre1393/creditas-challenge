import sys
import psycopg2
import numpy as np
import pandas as pd
import datetime as dt
import time

class DataPreparation:

	def __init__(self, transform_columns = {}, hide_apostrophe = True):
		self.transform_columns = transform_columns
		self.hide_apostrophe = hide_apostrophe

	def process(self, df):
		t0 = time.time()
		df['end_date'] = df['end_date'].apply(lambda x: dt.datetime.strptime(x, '%m/%d/%Y'))
		df['is_woman_owned'] = df['is_woman_owned'].apply(self.toBit)
		df['is_first_time_borrower'] = df['is_first_time_borrower'].apply(self.toBit)
		df = df.replace("'", "''", regex = True)
		t1 = time.time()
		print('preparacao dos dados executada em %.2f s' % (t1 - t0))

		return df

	def toBit(self, x):
		return 1 if x == 't' else 0