"""
A basic Python-based ETL program for working with spreadsheet data sources
"""

import os
from sqlalchemy import create_engine
from pandas import read_excel
import psycopg2


# Create custom connection function
def db_connection():

    # Define credentials using environment vars
    host = os.environ.get('POSTGRES_HOST')
    db = 'ETL_Test'
    user = os.environ.get('POSTGRES_USER')
    pgpass = os.environ.get('PG_PASS')

    connect = psycopg2.connect(database=db, host=host, user=user, password=pgpass)
    return connect


# Define engine
engine = create_engine('postgresql+psycopg2://', creator=db_connection)


# Define ETL operations
def etl():

    with read_excel('') as target:
        pass


def main():
    
    etl()


main()
