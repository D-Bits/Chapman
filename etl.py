"""
A basic Python-based ETL program for working with CSV data sources
"""

import csv, os, sqlalchemy as sql
import pandas as pd
import psycopg2 as pg
from json import dumps
from pandas.errors import DtypeWarning, EmptyDataError
from config import db_connection, db_engine, api_json  



# ETL for CSV files. "src" = csv file to extract from, 
# "table"= db table to load data into. 
def csv_etl(src, table):

    try:        
        df = pd.read_csv(src)
        df.to_sql(table, db_engine, index_label='id', if_exists='append')
        input(f'{len(df)} record(s) loaded into the database. Press enter to exit.')

    # Throw exception if data source is empty           
    except EmptyDataError:
        input('Error: No data in data source! Press enter to exit.')
    # Throw exception if data types are not compatible 
    except DtypeWarning:
        input('Error: Incompatible data type(s)! Press enter to exit.')


# ETL for Excel work books
def excel_etl(src, sheet, table):

    try:
        df = pd.read_excel(src, sheet_name=sheet)
        df.to_sql(table, db_engine, index_label='id', if_exists='append')
        input(f'{len(df)} record(s) loaded into the database. Press enter to exit.')

    # Throw exception if data source is empty
    except EmptyDataError:
        input('Error: No data in data source! Press enter to exit.')
    # Throw exception if data types are not compatible 
    except DtypeWarning:
        input('Error: Incompatible data type! Press enter to exit.')


# ETL for JSON datasets
def json_etl():

    data = dumps(api_json)
    df = pd.DataFrame.from_dict(api_json)

    df.head()

json_etl()