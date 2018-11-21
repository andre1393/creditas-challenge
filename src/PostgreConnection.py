import psycopg2
import pandas as pd
import datetime as dt
import time

class PostgreConnection():

	def __init__(self, thread_name, dbname, tablename, columns, conn_string):
		t0 = time.time()
		self.thread_name = thread_name
		self.dbname = dbname
		self.tablename = tablename
		self.columns = columns
		self.conn_string = conn_string
		self.conn = psycopg2.connect(self.conn_string)
		self.cur = self.conn.cursor()
		print('thread %s - conexao com o banco %s estabelecida com sucesso em %.2f s' % (self.thread_name, self.dbname, time.time() - t0))

	def load(self, df):
		t0 = time.time()

		placeholders = []
		for c in range(len(df.columns)):
			placeholders.append("'{}'")

		placeholders = "(" + ','.join(placeholders) + ")"

		# monta a string com todos os valores
		params = ','.join(placeholders.format(*x) for idx, x in df.iterrows()).replace("'nan'", 'NULL')
		
		self.cur.execute('INSERT INTO %s (%s) values %s' % (self.tablename, self.columns, params))
		self.conn.commit()
		t1 = time.time()
		print('thread %s - %d registros processados em %.2f s (%.2f registros/segundo)' % (self.thread_name, len(df), (t1 - t0), len(df)/(t1 - t0)))
