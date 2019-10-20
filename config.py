"""
File for storing various config info, such as
database credentials.
"""
from os import getenv
from psycopg2 import connect
from sqlalchemy import create_engine
from requests import get


# Define credentials for local Postgres instance
local_pghost = getenv('PG_HOST')
local_pguser = getenv('PG_USER')
local_pgpass = getenv('PG_PASS')
pgport = getenv('PG_PORT')

# Define credentials for AWS Postgres instance
aws_pghost = getenv('AWS_PG_HOST')
aws_pguser = getenv('AWS_PG_USER')
aws_pgpass = getenv('AWS_PG_PASS')

# Define credentials for AWS SQL Server instance
aws_msssql_host = getenv('AWS_MSSQL_HOST')
aws_mssql_user = getenv('AWS_MSSQL_USER')
aws_mssql_pass = getenv('AWS_MSSSQL_PASS')
aws_mssql_port = getenv('AWS_MSSSQL_PASS')

local_conn = connect(database='etl_test', host=local_pghost, user=local_pguser, password=local_pgpass)
aws_conn = connect(database='etl_test', host=local_pghost, user=local_pguser, password=local_pgpass)

# Create a SQLAlchemy engine for the local Postgres instance 
local_engine = create_engine(f"postgresql+psycopg2://{local_pguser}:{local_pgpass}@{local_pghost}:{pgport}/etl_test")
# Create a SQLAlchemy engine for the AWS Postgres instance 
aws_pg_engine = create_engine(f"postgresql+psycopg2://{aws_pguser}:{aws_pgpass}@{aws_pghost}:{pgport}/etl_test")


# API endpoint and key
api_key = getenv('MOCKAROO_API_KEY')
endpoint = get(f'https://my.api.mockaroo.com/super_secret_info.json?key={api_key}')
api_json = endpoint.json()

