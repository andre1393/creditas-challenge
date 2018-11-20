import sys
import psycopg2
import numpy as np
import pandas as pd
import datetime as dt
import time

class PostgreLoader:

	def __init__(self, dbname, columns, conn_string):
		t0 = time.time()
		self.dbname = dbname
		self.columns = columns
		self.conn_string = conn_string
		self.conn = psycopg2.connect(self.conn_string)
		self.cur = self.conn.cursor()
		print('conexao com o banco %s estabelecida com sucesso em %.2f s' % (self.dbname, time.time() - t0))

	def load(self, df):
		t0 = time.time()
		params = ','.join("('{}','{}',{},'{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')".format(*x) for idx, x in df.iterrows()).replace("'nan'", 'NULL')
		self.cur.execute('INSERT INTO transactions (%s) values %s' % (self.columns, params))
		self.conn.commit()
		t1 = time.time()
		print('%d registros processados em %.2f s (%.2f registros/segundo)' % (len(df), (t1 - t0), len(df)/(t1 - t0)))
