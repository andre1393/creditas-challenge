import datetime as dt
import time
import threading
from PostgreConnection import PostgreConnection
from DataPreparation import DataPreparation

class Pipeline(threading.Thread):

	def __init__(self, thread_name, dbname, columns, conn_string, df):
		super(Pipeline, self).__init__()
		self.thread_name = thread_name
		self.dbname = dbname
		self.columns = columns
		self.conn_string = conn_string
		self.df = df
		self.dataPreparation = DataPreparation(self.thread_name)
		self.loader = PostgreConnection(self.thread_name, dbname, columns, conn_string)
	
	def run(self):
		t0 = time.time()
		self.df = self.dataPreparation.process(self.df)
		self.loader.load(self.df)
		print('thread %s executada em %.2f s' % (self.thread_name, time.time() - t0))
