"""
A basic Python-based ETL program for working with CSV data sources
"""

import pandas as pd
from sqlalchemy.types import VARCHAR, Date
from pandas.errors import DtypeWarning, EmptyDataError
from pandas.io.json import json_normalize
from config import local_pg_engine, aws_pg_engine, aws_mssql_engine, api_json  
from requests.exceptions import HTTPError, ContentDecodingError, ConnectionError


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
        df.to_sql(table, local_pg_engine, index_label='id', dtype=data_types, if_exists='append')
        input(f'{len(df)} record(s) successfully loaded into "{table}". Press enter to exit.')

    # Throw exception if data source is empty           
    except EmptyDataError:
        input('Error: No data in data source! Press enter to exit.')
    # Throw exception if data types are not compatible 
    except DtypeWarning:
        input('Error: Incompatible data type(s)! Press enter to exit.')
    # Throw exception is data source cannot be found
    except FileNotFoundError:
        input('Error: Data source cannot be found. Press enter to exit.')


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
        df.to_sql(table, local_pg_engine, index_label='id', dtype=data_types, if_exists='append')
        input(f'{len(df)} record(s) successfully loaded into "{table}". Press enter to exit.')

    # Throw exception if data source is empty
    except EmptyDataError:
        input('Error: No data in data source! Press enter to exit.')
    # Throw exception if data types are not compatible 
    except DtypeWarning:
        input('Error: Incompatible data type! Press enter to exit.')
    # Throw exception is data source cannot be found
    except FileNotFoundError:
        input('Error: Data source cannot be found. Press enter to exit.')


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
        df.to_sql(table, local_pg_engine, dtype=data_types, if_exists='append')
        input(f'{len(df)} record(s) successfully loaded into "{table}". Press enter to exit.')

    # Throw exception if data source is empty
    except EmptyDataError:
        input('Error: No data in data source! Press enter to exit.')
    # Throw exception if data types are not compatible 
    except DtypeWarning:
        input('Error: Incompatible data type! Press enter to exit.')
    # Throw exception if there's an HTTP error
    except HTTPError:
        input('HTTP Error. Press enter to exit.')
    except ContentDecodingError:
        input('Content decoding error. Press enter to exit.')
    except ConnectionError:
        input('Connection Error. Press enter to exit.')


# Migrate a db table from a local Postgres instance to an AWS Postgres instance (Not yet working)
def aws_pg_migration(src_table, target_table):

    # Read from the source table, load into target table
    try: 
        data_src = pd.read_sql_table(src_table, local_pg_engine)
        data_src.to_sql(target_table, aws_pg_engine, index_label='id', if_exists='append')
        input(f'{len(data_src)} records were successfully loaded from the local "{src_table}" table into the AWS "{target_table}" table. Press enter to exit.')
    # Throw exception if data source is empty           
    except EmptyDataError:
        input('Error: No data in data source! Press enter to exit.')
    # Throw exception if data types are not compatible 
    except DtypeWarning:
        input('Error: Incompatible data type! Press enter to exit.')


# Migrate a db table from a local Postgres instance to an AWS SQL Server instance (Not yet working)
def aws_mssql_migration(src_table, target_table):

    # Read from the source table, load into target table
    try: 
        data_src = pd.read_sql_table(src_table, local_pg_engine)
        data_src.to_sql(target_table, aws_mssql_engine, index_label='id', if_exists='append')
        input(f'{len(data_src)} records were successfully loaded from the local "{src_table}" table into the AWS "{target_table}" table. Press enter to exit.')
    # Throw exception if data source is empty           
    except EmptyDataError:
        input('Error: No data in data source! Press enter to exit.')
    # Throw exception if data types are not compatible 
    except DtypeWarning:
        input('Error: Incompatible data type! Press enter to exit.')
 