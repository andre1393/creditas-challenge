# desafio-creditas
projeto de modelagem e ingestão de datasets em um banco de dados postgre

## Requisitos:
* Python 3.x
* postgre 11.1
* pip install psycopg2

## Como utilizar:
1. criar um arquivo .json seguindo o modelo presente na pasta ./config
2. criar um database e as tabelas conforme exemplo fornecido no arquivo ./scripts/create_tables.sql
3. executar o comando python ./src/postgre_loader.py ./config/{nome_arquivo_config}

## EDA:
- A resposta à algumas das perguntas do desafio estão no arquivo ./EDA/eda.html
