"""
A basic Python-based ETL program for working with CSV data sources
"""

import csv, os, sqlalchemy as sql
import pandas as pd
import psycopg2 as pg
from pandas.errors import DtypeWarning, EmptyDataError
from config import db_connection, db_engine  


# ETL for CSV files
def csv_etl(src, table):

    try:        
        df = pd.read_csv(src)
        df.to_sql(table, db_engine, index_label='id', if_exists='append')
        input('Data has finished loading into the database. Press enter to exit.')
        
    except EmptyDataError:
        input('Error: No data in data source! Press enter to exit.')
    except DtypeWarning:
        input('Error: Incompatible data type! Press enter to exit.')


# ETL for Excel work books
def excel_etl(src, sheet_name):

    try:
        df = pd.read_excel(src, sheet_name=sheet_name)
        df.to_sql('users', db_engine, index_label='id', if_exists='append')

    except EmptyDataError:
        input('Error: No data in data source! Press enter to exit.')
    except DtypeWarning:
        input('Error: Incompatible data type! Press enter to exit.')


# An older-methodoology, using raw SQL.
def raw_sql_etl():

    # Seed the users table w/ sample data from csv file
    with open('employees.csv', 'r') as data_file:
        reader = csv.reader(data_file)
        next(reader)  # Skip header row
        for row in reader:
            connection.execute( # Parameterize query to avoid SQL injections
                "INSERT INTO employees(fname, lname, email, street, city) VALUES(%s, %s, %s, %s, %s)",
                row
            )

        print('Query executed.')


excel_etl('users.xlsx', 'data')