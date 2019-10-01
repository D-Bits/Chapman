"""
File for storing various config info, such as
database credentials.
"""
from os import getenv
from psycopg2 import connect
from sqlalchemy import create_engine


# Define credentials using environment vars
pghost = getenv('POSTGRES_HOST')
pguser = getenv('POSTGRES_USER')
pgpass = getenv('PG_PASS')
pgport = getenv('POSTGRES_PORT')

db_connection = connect(database='ETL_Test', host=pghost, user=pguser, password=pgpass)

# Create a SQLAlchemy engine to write CSV data to the db
db_engine = create_engine(f"postgresql+psycopg2://{pguser}:{pgpass}@{pghost}:{pgport}/ETL_Test")