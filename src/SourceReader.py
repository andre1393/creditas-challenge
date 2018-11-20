import sys
import psycopg2
import numpy as np
import pandas as pd
import datetime as dt
import time

class SourceReader:

	def __init__(self, dataset, chunksize = None, names = None):
		self.dataset = dataset
		self.chunksize = chunksize
		self.names = names

	def read(self):	
		t0 = time.time()
		print('carregando dataset %s' % self.dataset)
		df_chunk = pd.read_csv(self.dataset, names = self.names, header = 0, chunksize = self.chunksize)
		print('dataset carregado em %.2f s' % (time.time() - t0))
		return df_chunk