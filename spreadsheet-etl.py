"""
A basic Python-based ETL program for working with spreadsheet data sources
"""
from sqlalchemy import create_engine
import pandas as pd
from pandas import ExcelFile
import psycopg2
import os


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

        data_src = pd.ExcelFile('world-ports.xlsx')
        ports = data_src.parse('ports') # Parse data from the 'ports' sheet

        for row in ports:
            engine.execute(
                "INSERT INTO ports(city, MetroPop, AnnualCargo) VALUES(%s, %s, %s)",
                row
            )


def main():

    etl()


main()
