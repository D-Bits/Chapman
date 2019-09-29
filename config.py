"""
File for storing various config info, such as
database credentials.
"""
from os import getenv
from psycopg2 import connect
from sqlalchemy import create_engine


# Define credentials using environment vars
pghost = getenv('POSTGRES_HOST')
pgport = getenv('POSTGRES_PORT')
pguser = getenv('POSTGRES_USER')
pgpass = getenv('PG_PASS')

