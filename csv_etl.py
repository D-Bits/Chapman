"""
A basic Python-based ETL program for working with CSV data sources
"""

import csv, os, sqlalchemy as sql
from sqlalchemy import create_engine
import psycopg2

# Create custom connection function
def db_connection():

    # Define credentials using environment vars
    host = os.environ.get('POSTGRES_HOST')
    db = 'ETL_Test'
    user = os.environ.get('POSTGRES_USER')
    pgpass = os.environ.get('PG_PASS')

    connection = psycopg2.connect(database=db, host=host, user=user, password=pgpass)
    return connection

# Define engine
engine = create_engine('postgresql+psycopg2://', creator=db_connection)

# Define ETL operations
def etl():
    # Seed the users table w/ sample data from csv file
    with open('data.csv', 'r') as data_file:
        reader = csv.reader(data_file)
        next(reader) # Skip header row
        for row in reader:
            engine.execute( # Parameterize query to avoid SQL injections
                "INSERT INTO users(email, lname, fname, homeaddress) VALUES(%s, %s, %s, %s)",
                row
            )

def main():

    db_connection()
    etl()

main()