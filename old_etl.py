from config import db_connection 
import csv


# An older-methodoology, using raw SQL.
def raw_sql_etl():

    # Seed the users table w/ sample data from csv file
    with open('employees.csv', 'r') as data_file:
        reader = csv.reader(data_file)
        next(reader)  # Skip header row
        for row in reader:
            db_connection.execute( # Parameterize query to avoid SQL injections
                "INSERT INTO employees(fname, lname, email, street, city) VALUES(%s, %s, %s, %s, %s)",
                row
            )

        print('Query executed.')