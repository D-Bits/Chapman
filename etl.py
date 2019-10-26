"""
A basic Python-based ETL program for working with CSV data sources
"""
import pandas as pd
from sqlalchemy.types import VARCHAR, Date, BigInteger
from psycopg2.errors import NoDataFound
from pandas.errors import DtypeWarning, EmptyDataError, PerformanceWarning
from pandas.io.json import json_normalize
from config import local_pg_engine, aws_pg_engine, aws_mssql_engine, local_pg_creds, local_pg_conn, server_listing_json
from requests.exceptions import HTTPError, ContentDecodingError, ConnectionError


# ETL for CSV files. "src" = csv file to extract from, 
# "table"= db table to load data into. 
def csv_etl(src, table):

    try: 
        # Create a cursor
        curs = local_pg_conn.cursor()

        with open(src, 'r') as data_src:

            # Skip header row 
            next(data_src)

            # Load data from CSV into db
            curs.copy_from(data_src, table, sep=',')

            # Commit the transaction to the database, and close the connection
            local_pg_conn.commit()
            local_pg_conn.close()

            input(f'Record(s) successfully loaded into the "{table}" table in "{local_pg_creds["host"]}". Press enter to exit.')
    
    # Throw exception is data source cannot be found
    except FileNotFoundError:
        input('Error: Data source cannot be found. Press enter to exit.')

    # Throw exception if data source is empty           
    except NoDataFound:
        input('Error: No data in data source! Press enter to exit.')


# ETL for Excel work books
def excel_etl(src, sheet, table):

    try:
        data_src = pd.read_excel(src, sheet_name=sheet)
        df = data_src.set_index('id')
        df.to_sql(table, aws_pg_engine, index_label='id', if_exists='append')
        input(f'{len(df)} record(s) successfully loaded into the "{table}" table in "{local_pg_creds["host"]}". Press enter to exit.')

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

    # "Flatten" the JSON data into a columnar format
    flattened_json = json_normalize(server_listing_json)
    # load flattened JSON into a DataFrame
    data = pd.DataFrame(flattened_json)
    df = data.set_index('id')

    try:
        df.to_sql(table, local_pg_engine, index_label='id', if_exists='append')
        input(f'{len(df)} record(s) successfully loaded into the "{table}" table in {local_pg_creds["host"]}. Press enter to exit.')

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

    # Define data types for the users table
    data_types = {
        "last_name": VARCHAR(255),
        "first_name": VARCHAR(255),
        "email": VARCHAR(255),
        "street": VARCHAR(255),
        "city": VARCHAR(255),
        "state": VARCHAR(255),
    }

    # Read from the source table, load into target table
    try: 
        data_src = pd.read_sql_table(src_table, local_pg_engine, index_col='id')
        df = data_src.set_index='id'
        df.to_sql(target_table, aws_pg_engine, index_label='id', if_exists='append')
        input(f'{len(data_src)} records were successfully loaded from the local "{src_table}" table into the AWS "{target_table}" table. Press enter to exit.')
    # Throw exception if data source is empty           
    except EmptyDataError:
        input('Error: No data in data source! Press enter to exit.')
    # Throw exception if data types are not compatible 
    except DtypeWarning:
        input('Error: Incompatible data type! Press enter to exit.')


# Migrate a db table from a local Postgres instance to an AWS SQL Server instance (Not yet working)
def aws_mssql_migration(src_table, target_table):

    # Define data types for the table
    data_types = {
        "last_name": VARCHAR(255),
        "first_name": VARCHAR(255),
        "email": VARCHAR(255),
        "street": VARCHAR(255),
        "city": VARCHAR(255),
        "state": VARCHAR(255),
    }

    # Read from the source table, load into target table
    try: 
        data_src = pd.read_sql_table(src_table, local_pg_engine)
        df = data_src.set_index('id')
        df.to_sql(target_table, aws_mssql_engine, index_label='id', dtype=data_types, if_exists='append')
        input(f'{len(data_src)} records were successfully loaded from the local "{src_table}" table into the AWS "{target_table}" table. Press enter to exit.')
    # Throw exception if data source is empty           
    except EmptyDataError:
        input('Error: No data in data source! Press enter to exit.')
    # Throw exception if data types are not compatible 
    except DtypeWarning:
        input('Error: Incompatible data type! Press enter to exit.')
 