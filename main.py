from etl import ETL


# Store the user's options in a dictionary
u_options = {
    1: 'Do ETL with a CSV file.',
    2: 'Do ETL with an Excel spreadsheet.',
    3: "Load JSON data from an API into a database.",
    4: "Migrate database tables to an GCP Postgres db.",
    5: "Migrate database tables to an GCP MSSQL db."
}


if __name__ == "__main__":
    
    print()

    for key, val in u_options.items():

        print(key, val)

    print()

    # Prompt the user to enter a choice
    u_choice = int(input('Enter an integer, based on the above options: '))

    if u_choice == 1:
        # Prompt the user to enter a file name
        file_name = input('Enter a full path, and a CSV file name, with the extension (Ex: "data.csv"): ')
        table_name = input('Input the name of the table in the database that you want to load data into: ')

        if file_name is None:
            raise Exception('File name cannot be null!')
        elif table_name is None:
            raise Exception('Must specify a table in the database!')
        else:
            ETL.csv_etl(file_name, table_name)

    elif u_choice == 2:
        # Prompt the user to enter a file name, and a sheet name
        file_name = input('Enter a full path, and file name for your Excel workbook, with the extension (Ex: "/username/home/documents/info.xlsx"): ')
        sheet_name = input('Enter a name for the sheet in your workbook that you would like to extract data from (Ex: "Sheet1"): ')
        table_name = input('Input the name of the table in the database that you want to load data into: ')

        if file_name is None:
            raise Exception('File and/or sheet name cannot be null!')
        elif sheet_name is None:
            raise Exception('Sheet name cannot be null!')
        elif table_name is None:
            raise Exception('Table name cannot be null!')
        else:
           ETL.excel_etl(file_name, sheet_name, table_name)

    elif u_choice == 3:
        table_name = input('Input the name of the table in the database that you want to load data into: ')
        if table_name is None:
            raise Exception('Table name cannot be null!')
        else:
           ETL.json_etl(table_name)

    elif u_choice == 4:
        local_table = input('Enter the name of a table in a local database to migrate: ')
        aws_table = input('Enter an table in your AWS Postgres database that you want to migrate to: ')
        if local_table is None:
            raise Exception('Must specify source table!')
        elif aws_table is None:
            raise Exception('Must specify target table!')
        else:
           ETL.aws_pg_migration(local_table, aws_table)
    
    elif u_choice == 5:
        local_table = input('Enter the name of a table in a local database to migrate: ')
        aws_table = input('Enter an table in your AWS SQL Server database that you want to migrate to: ')
        if local_table is None:
            raise Exception('Must specify source table!')
        elif aws_table is None:
            raise Exception('Must specify target table!')
        else:
           ETL.aws_mssql_migration(local_table, aws_table)

    else:
        input('Invalid value entered. Press enter to exit.')
