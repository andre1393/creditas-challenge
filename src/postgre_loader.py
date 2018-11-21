import sys
import numpy as np
import pandas as pd
import datetime as dt
import time

from SourceReader import SourceReader
from Pipeline import Pipeline
import threading
import json

def main(args):
	if len(args) < 1:
		print('usage: informe o nome do arquivo de configuracao')
		return

	with open(args[0]) as content:
		configs = json.load(content)

	conn_string = 'host={} dbname={} user={} password={}'.format(configs['host'], configs['dbname'], configs['user'], configs['password'])

	t0 = time.time()

	# carregando o dataset
	reader = SourceReader(configs['dataset'], chunksize = configs['chunksize'], names = configs['columns'], codec = configs['codec'])
	df_chunk = reader.read()
	
	thread = 0
	pipelines = {}
	for df in df_chunk:
		pipelines[thread] = Pipeline(str(thread), configs['dbname'], configs['tablename'], ','.join(configs['columns']), conn_string, df, configs['preprocess'], '{}.error'.format(configs['dataset']))
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