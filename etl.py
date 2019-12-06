"""
A basic Python-based ETL program for working with CSV data sources
"""
import pandas as pd
from sqlalchemy.types import VARCHAR, Date, BigInteger
from psycopg2.errors import NoDataFound, UndefinedTable
from pandas.errors import DtypeWarning, EmptyDataError, PerformanceWarning
from pandas.io.json import json_normalize
from os import remove
from config import(
    local_pg_engine, 
    aws_pg_engine,
    aws_pg_conn, 
    aws_mssql_engine, 
    local_pg_creds, 
    local_pg_conn,
    server_listing_json, 
    aws_pg_creds, 
    stocks_json
) 
from requests import get
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

            # Count to no. of rows loaded into the db
            row_count = curs.execute(f'SELECT COUNT(*) FROM {table};')

            # Close the connection
            local_pg_conn.close()
            

        input(f'{row_count} record(s) successfully loaded into the "{table}" table in "{local_pg_creds["database"]}" on "{local_pg_creds["host"]}". Press enter to exit.')
    
    # Throw exception is data source cannot be found
    except FileNotFoundError:
        input('Error: Data source cannot be found! Press enter to exit.')

    # Throw exception if data source is empty           
    except NoDataFound:
        input('Error: No data in data source! Press enter to exit.')

    # Throw exception if table does not exist in DB.
    except UndefinedTable:
        input('Error: Table does not exist! Press enter to exit.')


# ETL for Excel work books
def excel_etl(src, sheet, table):

    try:
        data_src = pd.read_excel(src, sheet_name=sheet)
        df = data_src.set_index('id', append=False)
        df.to_sql(table, local_pg_engine, index_label='id', if_exists='append')
        input(f'{len(df)} record(s) successfully loaded into the "{table}" table in "{local_pg_creds["host"]}". Press enter to exit.')

    # Throw exception if data source is empty
    except EmptyDataError:
        input('Error: No data in data source! Press enter to exit.')
    # Throw exception if data types are not compatible 
    except DtypeWarning:
        input('Error: Incompatible data type! Press enter to exit.')
    # Throw exception is data source cannot be found
    except FileNotFoundError:
        input('Error: Data source cannot be found! Press enter to exit.')
    # Throw exception if table does not exist in DB.
    except UndefinedTable:
        input('Error: Table does not exist in database! Press enter to exit.')


# ETL for JSON datasets from an API
def json_etl(table):

    try:
        # "Flatten" the JSON data into a columnar format
        flattened_json = json_normalize(server_listing_json)
        # load flattened JSON into a DataFrame
        data = pd.DataFrame(flattened_json)
        df = data.set_index('id')

        df.to_sql(table, local_pg_engine, index_label='id', if_exists='append')
        input(f'{len(df)} record(s) successfully loaded into the "{table}" table in "{local_pg_creds["database"]}" on "{local_pg_creds["host"]}". Press enter to exit.')
    
    # Throw exception if data source is empty
    except EmptyDataError:
        input('Error: No data in data source! Press enter to exit.')
    # Throw exception if data types are not compatible 
    except DtypeWarning:
        input('Error: Incompatible data type! Press enter to exit.')
    # Throw exception if there's an HTTP error
    except HTTPError:
        input(f'{HTTPError.errno} Error. Press enter to exit.')
    except ContentDecodingError:
        input('Content decoding error. Press enter to exit.')
    except ConnectionError:
        input('Connection Error. Press enter to exit.')
    # Throw exception if table does not exist in DB.
    except UndefinedTable:
        input('Error: Table does not exist in database! Press enter to exit.')


# Migrate a db table from a local Postgres instance to an AWS Postgres instance 
def aws_pg_migration(src_table, target_table):

    try:
        # Extract the data, and dump it to a temporary CSV file
        df = pd.read_sql_table(src_table, local_pg_engine, index_col='id')
        df.to_csv('data/temp/sql_dump.csv', sep=',')
            
        # Create a cursor
        curs = aws_pg_conn.cursor()

        with open('data/temp/sql_dump.csv', 'r', encoding="utf8") as data_src:

            # Skip header row 
            next(data_src)
                    
            # Load data from CSV into db
            curs.copy_from(data_src, target_table, sep=',')

            # Commit the transaction to the database, and close the connection
            aws_pg_conn.commit()
            aws_pg_conn.close()
        
        # Delete the CSV file, as it is no long necessary
        remove('data/temp/sql_dump.csv')

        # Show the user how many records were migrated, and terminate program.
        input(f'{len(df)} record(s) successfully loaded into the "{target_table}" table in "{aws_pg_creds["database"]}" on "{aws_pg_creds["host"]}". Press enter to exit.')

    # Throw exception if data source is empty           
    except EmptyDataError:
        input('Error: No data in data source! Press enter to exit.')
    # Throw exception if data types are not compatible 
    except DtypeWarning:
        input('Error: Incompatible data type! Press enter to exit.')
    # Throw exception if table does not exist in DB.
    except UndefinedTable:
        input('Error: Table does not exist in database! Press enter to exit.')


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
    # Throw exception if table does not exist in DB.
    except UndefinedTable:
        input('Error: Table does not exist in database! Press enter to exit.')
    # Throw erro if temp CSV file does not exist
    except FileNotFoundError:
        input('Error: Data source cannot be found! Press enter to exit.')
 