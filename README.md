# code-challenge_indicium

Para realizar o desafio e montar a pipeline, utilizei um código em python, com as bibliotecas pandas, sqlalchemy, psycopg2, os, datetime, pathlib, sys. Foi usado também um arquivo .csv e um banco de dados postgres.

## Banco de dados

Para ter o bando de dados disponível e o usar para o desafio é preciso executar esse comando:

docker-compose up

## Bibliotecas

Para importar as bibliotecas utilizadas é preciso executar esses comandos:

pip install pandas

pip install psycopg2

pip install sqlalchemy

## Execução do pipeline

O código foi feito para oferecer a opção do pipeline ser executado de forma parcial.
Para executar apenas a etapa 1 e utilizar a data atual utilize o código:

python codigo.py 1

Para executar apenas a etapa 2 e utilizar a data atual utilize o código:

python codigo.py 2

Para executar a etapa 1 e 2 juntas e utilizar a data atual utilize o código:

python codigo.py  1,2 ou python codigo.py 2,1 

Caso queira fazer o pipeline de alguma data anterior específica basta colocar ela no final no seguinte formato 'ano-mes-dia'


python codigo.py 1,2 2022-02-07






