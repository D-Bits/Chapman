"""
File for storing various config info, such as
database credentials.
"""
from os import getenv
from psycopg2 import connect
from sqlalchemy import create_engine
from requests import get


# Define credentials for local Postgres instance
local_pg_creds = {
    'host': getenv('PG_HOST'),
    'user': getenv('PG_USER'),
    'password': getenv('PG_PASS'),
    'port': getenv('PG_PORT'),
}

# Define credentials for AWS Postgres instance
aws_pg_creds = {
    'host': getenv('AWS_PG_HOST'),
    'user': getenv('AWS_PG_USER'),
    'password': getenv('AWS_PG_PASS'),
}


# Create a SQLAlchemy engine for the local Postgres instance 
local_pg_engine = create_engine(f"postgresql+psycopg2://{local_pg_creds['user']}:{local_pg_creds['password']}@{local_pg_creds['host']}:{local_pg_creds['port']}/etl_test")
# Create a SQLAlchemy engine for the AWS Postgres instance 
aws_pg_engine = create_engine(f"postgresql+psycopg2://{aws_pg_creds['user']}:{aws_pg_creds['password']}@{aws_pg_creds['host']}:{local_pg_creds['port']}/etl_test")

# Define credentials for AWS SQL Server instance
aws_mssql_creds = {
    'host': getenv('AWS_MSSQL_HOST'),
    'user': getenv('AWS_MSSQL_USER'),
    'password': getenv('AWS_MSSQL_PASS'),
    'port': getenv('AWS_MSSQL_PORT'),
}

# Create a SQLAlchemy engine for the AWS SQL Server instance 
aws_mssql_engine = create_engine(f"mssql+pyodbc://{aws_mssql_creds['user']}:{aws_mssql_creds['password']}@{aws_mssql_creds['host']}:{aws_mssql_creds['port']}/etl_test?driver=SQL+Server+Native+Client+10.0") 


# API endpoint and key
api_key = getenv('MOCKAROO_API_KEY')
endpoint = get(f'https://my.api.mockaroo.com/super_secret_info.json?key={api_key}')
api_json = endpoint.json()

