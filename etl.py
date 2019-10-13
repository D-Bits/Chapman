"""
A basic Python-based ETL program for working with CSV data sources
"""

import os, sqlalchemy as sql
import pandas as pd
from sqlalchemy.types import VARCHAR, Date
from pandas.errors import DtypeWarning, EmptyDataError
from pandas.io.json import json_normalize
from config import db_connection, db_engine, api_json  


# ETL for CSV files. "src" = csv file to extract from, 
# "table"= db table to load data into. 
def csv_etl(src, table):

    # Define data types for the table
    data_types = {
        "last_name": VARCHAR(255),
        "first_name": VARCHAR(255),
        "email": VARCHAR(255),
        "street": VARCHAR(255),
        "city": VARCHAR(255),
        "state": VARCHAR(255),
    }

    try:        
        df = pd.read_csv(src)
        df.to_sql(table, db_engine, index_label='id', dtype=data_types, if_exists='append')
        input(f'{len(df)} record(s) successfully loaded into the database. Press enter to exit.')

    # Throw exception if data source is empty           
    except EmptyDataError:
        input('Error: No data in data source! Press enter to exit.')
    # Throw exception if data types are not compatible 
    except DtypeWarning:
        input('Error: Incompatible data type(s)! Press enter to exit.')


# ETL for Excel work books
def excel_etl(src, sheet, table):

    # Define data types for the table
    data_types = {
        "last_name": VARCHAR(255),
        "first_name": VARCHAR(255),
        "email": VARCHAR(255),
        "street": VARCHAR(255),
        "city": VARCHAR(255),
        "state": VARCHAR(255),
    }

    try:
        df = pd.read_excel(src, sheet_name=sheet)
        df.to_sql(table, db_engine, index_label='id', dtype=data_types, if_exists='append')
        input(f'{len(df)} record(s) successfully loaded into the database. Press enter to exit.')

    # Throw exception if data source is empty
    except EmptyDataError:
        input('Error: No data in data source! Press enter to exit.')
    # Throw exception if data types are not compatible 
    except DtypeWarning:
        input('Error: Incompatible data type! Press enter to exit.')


# ETL for JSON datasets from an API
def json_etl(table):

    # Define data types for the table
    data_types = {
        "username": VARCHAR(255),
        "data": VARCHAR(255),
        "data_hash": VARCHAR(255),
        "last_updated": Date,
        "city": VARCHAR(255),
        "state": VARCHAR(255),
    }

    # "Flatten" the JSON data into a columnar format
    flattened_json = json_normalize(api_json)
    # load flattened JSON into a DataFrame
    df = pd.DataFrame(flattened_json)

    try:
        df.to_sql(table, db_engine, dtype=data_types, if_exists='append')
        input(f'{len(df)} record(s) successfully loaded into the database. Press enter to exit.')

    # Throw exception if data source is empty
    except EmptyDataError:
        input('Error: No data in data source! Press enter to exit.')
    # Throw exception if data types are not compatible 
    except DtypeWarning:
        input('Error: Incompatible data type! Press enter to exit.')
