# Importando bibliotecas
import pandas as pd
import psycopg2 as pg
from sqlalchemy import create_engine
import os
from datetime import date
from pathlib import Path
import sys


# Data atual
today=date.today()

########### Etapa 1 ###########
def step1(data):
    # Conexão com bando de dados (psycopg2)
    conn = pg.connect(user = "northwind_user", password = "thewindisblowing", host = "localhost", port = "5432", database = 'northwind')

    # Criando cursor
    cursor=conn.cursor()
    cursor.execute("SELECT table_schema, table_name FROM information_schema.tables WHERE(table_schema = 'public')ORDER BY table_schema, table_name;")
    list_tables = cursor.fetchall()

    # Lendo as tabelas do Banco de dados (Northwind) e Criando arquivos.csv de cada tabela.
    for t_name_table in list_tables:
        table_name = t_name_table[1] 
        df = pd.read_sql(f"select * from {table_name}",conn) # Lendo tabelas do database
        Path(f"./data/postgres/{table_name}/{data}").mkdir(parents=True, exist_ok=True) # Criando diretórios para cada Tabela
        df.to_csv(f"./data/postgres/{table_name}/{data}/{table_name}.csv",sep=",")

    # Lendo os dados da tabela (order_details.csv)
    tabela = pd.read_csv("./data/order_details.csv",sep=",")

    # Criando diretório csv
    Path(f"./data/csv/{data}").mkdir(parents=True,exist_ok=True)

    # Criando tabela com nome correto
    tabela.to_csv(f"./data/csv/{data}/order_details.csv")

########### Etapa 2 ###########
def step2(data):
    # Checar se a existe a pasta com a "data" 
    # Iniciando conexão com northwind (create_engine)
    engine = create_engine('postgresql://northwind_user:thewindisblowing@localhost:5432/northwind',isolation_level='AUTOCOMMIT')
    connection=engine.connect()
    # Criar o Banco de dados (database_teste), caso já tenha sido criado não fazer nada 
    try:
        connection.execute('CREATE DATABASE database_teste')
    except:
        pass

    # Iniciando conexão com database_teste
    engine = create_engine('postgresql://northwind_user:thewindisblowing@localhost:5432/database_teste',isolation_level='AUTOCOMMIT')
    connection=engine.connect() 

    # Lendo tabela de order_details e orders
    df_orderDetails=pd.read_csv(f"./data/csv/{data}/order_details.csv",sep=",")
    df_orders=pd.read_csv(f"./data/postgres/orders/{data}/orders.csv",sep=",")

    # Carregando os dados para database_teste
    df_orderDetails.to_sql(f'order_details',con=engine,if_exists='append')
    df_orders.to_sql(f'orders_table',con=engine,if_exists='append')

    # Selecionando os atributos que serão usados
    atrib="orders_table.order_id, customer_id,employee_id,order_date,required_date,shipped_date,ship_via,freight,ship_name,ship_address,ship_city,ship_region,ship_postal_code,ship_country,product_id,unit_price,quantity,discount"
    
    # Fazendo o JOIN para juntar as tabelas de acordo com order_id
    dataFrame=pd.read_sql_query(f"SELECT {atrib} FROM orders_table RIGHT JOIN order_details ON orders_table.order_id = order_details.order_id",con=engine)
    
    # Criando o arquivo csv das tabelas juntas.
    dataFrame.to_csv("./data/dataFrame.csv")

# x = qual etapa será realizada(1,2 ou 1 e 2)
x = sys.argv[1].split(',')

# pipeline_date = valor passado como argumento ou valor da data atual como default
if len(sys.argv) == 3:
  pipeline_date = sys.argv[2].split(",")
else:
    pipeline_date=today

# Função para organizar o sistema
def call_step(x,pipeline_data):
     if "1" in x:
           step1(pipeline_data)
     if "2" in x:
        if os.path.isdir((f"./data/postgres/orders/{pipeline_data}")and(f"./data/csv/{pipeline_data}")):
                step2(pipeline_data)
        else:
                print("Tabela atual não foi criada, execute o step1")

# Chamada da função
call_step(x,pipeline_date)