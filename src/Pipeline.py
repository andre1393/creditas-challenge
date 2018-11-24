import datetime as dt
import time
import threading
from PostgreConnection import PostgreConnection
from DataPreparation import DataPreparation
import pandas as pd
import os

class Pipeline(threading.Thread):

	def __init__(self, thread_name, dbname, tablename, columns, conn_string, df, preprocess, error_output):
		super(Pipeline, self).__init__()
		self.thread_name = thread_name
		self.dbname = dbname
		self.tablename = tablename
		self.columns = columns
		self.conn_string = conn_string
		self.df = df
		self.preprocess = preprocess
		self.error_output = error_output
		self.dataPreparation = DataPreparation(self.thread_name, preprocess = self.preprocess)
		self.loader = PostgreConnection(self.thread_name, self.dbname, self.tablename, self.columns, self.conn_string)

	def run(self):
		t0 = time.time()
		try:
			# executa o pre processamento dos dados
			self.df_processed = getattr(DataPreparation, self.dataPreparation.preprocess.get('method'))(self.dataPreparation, self.df)
			# salva os registros no banco de dados
			self.loader.load(self.df_processed)
			print('thread %s executada em %.2f s' % (self.thread_name, time.time() - t0))
		except Exception as err:
			print('thread %s - falha no processamento' % self.thread_name)
			print('Excessao %s: %s' % (type(err), err))
			
			# se o arquivo nao existe, coloca o header
			if not os.path.isfile(self.error_output):
   				self.df.to_csv(self.error_output, mode = 'a', index = False, header = True)
			else: # se ja existe nao coloca o header
   				self.df.to_csv(self.error_output, mode = 'a', index = False, header = False)
			
