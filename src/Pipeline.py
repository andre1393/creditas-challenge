import datetime as dt
import time
import threading
from PostgreConnection import PostgreConnection
from DataPreparation import DataPreparation
import pandas as pd
import os

class Pipeline(threading.Thread):

	def __init__(self, thread_name, dbname, columns, conn_string, df, error_output):
		super(Pipeline, self).__init__()
		self.thread_name = thread_name
		self.dbname = dbname
		self.columns = columns
		self.conn_string = conn_string
		self.df = df
		self.dataPreparation = DataPreparation(self.thread_name)
		self.loader = PostgreConnection(self.thread_name, dbname, columns, conn_string)
		self.error_output = error_output

	def run(self):
		t0 = time.time()
		try:
			self.df_processed = self.dataPreparation.process(self.df)
			self.loader.load(self.df_processed)
			print('thread %s executada em %.2f s' % (self.thread_name, time.time() - t0))
		except Exception as err:
			print('thread %s - falha no processamento' % self.thread_name)
			print('Excessao %s: %s' % (type(err), err))
			# if file does not exist write header 
			if not os.path.isfile(self.error_output):
   				self.df.to_csv(self.error_output, mode = 'a', index = False, header = True)
			else: # else it exists so append without writing the header
   				self.df.to_csv(self.error_output, mode = 'a', index = False, header = False)
			
