import os
import pandas as pd
import sqlite3
import re


# Establishing a connection to an in-memory SQLite database
def establish_connection():
    conn = sqlite3.connect(':memory:')# Connect to an in-memory SQLite database and create a cursor to execute SQL commands
    cursor = conn.cursor()
    return conn, cursor

# Function to load data from CSV files into SQLite tables
def load_data_to_sqlite(file_path, table_name, connection):
    df = pd.read_csv(file_path) # Read data from the CSV file into a DataFrame
    df.to_sql(table_name, connection, if_exists='replace', index=False)# Write the DataFrame to an SQLite table

# Function to create SQLite file paths for the tables and load data into SQLite tables
def create_sql_file_paths(file_dir, data_files, table_names):
    for file, table in zip(data_files, table_names):
        file_path = os.path.join(file_dir, file)  # Create the file path for each CSV file
        load_data_to_sqlite(file_path, table, sqlite3.connect(os.path.join(file_dir, table + '.db'))) # Load data from the CSV file into the corresponding SQLite table

# Function to attach databases to the in-memory SQLite connection
def attach_databases(file_dir, table_names):
    conn, cursor = establish_connection()  # Establish the in-memory SQLite connection
    for db_name in table_names:
        cursor.execute(f"ATTACH DATABASE '{os.path.join(file_dir, db_name)}.db' AS {db_name}") # Attach individual SQLite databases to the connection
    return conn, cursor  # Return the connection and cursor for further operations


# Function to get user inputs for the SQL query
def get_user_inputs():
    while True:
        client = int(input("Enter the client number: "))
        po_number = int(input("Enter PO Number: "))
        update_date = input("Enter Date (YYYY-MM-DD): ")

        if re.match(r"\d{4}-\d{2}-\d{2}", update_date):
            break  # Break the loop if the format is correct
        else:
            print("The date input has incorrect format. Please re-enter.")
    return client, po_number, update_date


# Function to perform the SELECT query with user inputs
def perform_query(cursor):
    query = '''
    SELECT
        EKPO.MANDT AS client,
        EKPO.EBELN AS po_number,
        SUM(DISTINCT EKPO.NETWR) AS total_unique_net_value,
        date(SUBSTR(CDHDR.UDATE, 1, 10)) AS update_date
    FROM
        EKPO
    LEFT JOIN
        EKKO ON EKPO.MANDT = EKKO.MANDT AND EKPO.EBELN = EKKO.EBELN
    LEFT JOIN
        CDHDR ON EKPO.MANDT = CDHDR.MANDANT AND EKPO.EBELN = CDHDR.OBJECTID
    LEFT JOIN
        CDPOS ON EKPO.MANDT = CDPOS.MANDANT AND EKPO.EBELN = CDPOS.OBJECTID
    WHERE
        EKPO.MANDT=? AND EKPO.EBELN = ?
        AND date(SUBSTR(CDHDR.UDATE, 1, 10)) >= (
            SELECT MAX(date(SUBSTR(CDHDR.UDATE, 1, 10)))
            FROM CDHDR
            WHERE date(SUBSTR(CDHDR.UDATE, 1, 10)) <= ?)
    GROUP BY
        update_date
    ORDER BY
        update_date;
    '''

    cursor.execute(query, get_user_inputs())  # Execute the query with user inputs
    result = cursor.fetchall()  # Fetch the query result
    return result

def complete_workflow(file_dir, data_files, table_names):
    create_sql_file_paths(file_dir, data_files, table_names)
    conn, cursor = attach_databases(file_dir, table_names)
    query_result = perform_query(cursor)
    for row in query_result:
        print(row)
    conn.close()

def get_сsv_file_paths(data_files, table_names):
    file_paths = [os.path.join(file_dir, file) for file in data_files]
    return file_paths

file_dir = os.getcwd()  

table_names = ['EKPO', 'EKKO', 'CDHDR', 'CDPOS']
data_files = [format+".csv" for format in table_names]

complete_workflow(file_dir, data_files, table_names)
result_paths=get_сsv_file_paths(data_files, table_names)