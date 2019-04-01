# Creditas challenge

This project was developed as part of the Creditas selection process. It aims to do modeling and ingesting data in a postgre database using python.

## Requirements:
* Python 3.x
* postgre 11.1
* pip install psycopg2

## How to use:
1. Create a json file following the model in the ./config folder.
2. Create a database and the table using the sql script provided in ./scripts/create_tables.sql
3. Run the command "python ./src/postgre_loader.py ./config/{config_file_name}

## EDA:
- The answer of some questions in the challenge description are in the ./EDA/eda.html
