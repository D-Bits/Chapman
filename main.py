from etl import csv_etl, excel_etl


# Store the user's options in a dictionary
u_options = {
    1: 'Do ETL with a CSV file.',
    2: 'Do ETL with an Excel spreadsheet.',
}


if __name__ == "__main__":
    
    print()

    for key, val in u_options.items():

        print(key, val)

    print()

    # Prompt the user to enter a choice
    u_choice = int(input('Enter an integer, based on the above options: '))

    if u_choice == 1:
        # Promt the user to enter a file name
        file_name = input('Enter a CSV file name, with the extension (Ex: "data.csv"): ')
        table_name = input('Input the name of the table in the database that you want to load data into: ')
        if file_name is None:
            raise Exception('File name cannot be null!')
        elif table_name is None:
            raise Exception('Must specify a table in the database!')
        else:
            csv_etl(file_name, table_name)

    elif u_choice == 2:
        # Promt the user to enter a file name, and a sheet name
        file_name = input('Enter a file name for your Excel workbook, with the extension (Ex: "info.xlsx"): ')
        sheet_name = input('Enter a name for the sheet in your workbook that you would like to extract data from (Ex: "Sheet1"): ')
        table_name = input('Input the name of the table in the database that you want to load data into: ')
        if file_name is None:
            raise Exception('File and/or sheet name cannot be null!')
        elif sheet_name is None:
            raise Exception('Sheet name cannot be null!')
        elif table_name is None:
            raise Exception('Table name cannot be null!')
        else:
            excel_etl(file_name, sheet_name, table_name)