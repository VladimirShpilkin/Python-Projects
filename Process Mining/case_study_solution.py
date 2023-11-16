import os
import pandas as pd
import sqlite3

def establish_connection():
    conn = sqlite3.connect(':memory:')
    cursor = conn.cursor()
    return conn, cursor

def load_data_to_sqlite(file_path, table_name, connection):
    df = pd.read_csv(file_path)
    df.to_sql(table_name, connection, if_exists='replace', index=False)

def create_file_paths(file_dir, data_files, table_names):
    for file, table in zip(data_files, table_names):
        file_path = os.path.join(file_dir, file)
        load_data_to_sqlite(file_path, table, sqlite3.connect(os.path.join(file_dir, table + '.db')))

def attach_databases(file_dir, table_names):
    conn, cursor = establish_connection()
    for db_name in table_names:
        cursor.execute(f"ATTACH DATABASE '{os.path.join(file_dir, db_name)}.db' AS {db_name}")

    return conn, cursor

def get_user_inputs():
    client = int(input("Enter the client number: "))
    po_number = int(input("Enter PO Number: "))
    update_date = input("Enter Update Date (YYYY-MM-DD): ")
    return client, po_number, update_date

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
        EKPO.MANDT=? AND EKPO.EBELN = ? AND date(SUBSTR(CDHDR.UDATE, 1, 10)) >= (
            SELECT MAX(date(SUBSTR(CDHDR.UDATE, 1, 10)))
            FROM CDHDR
            WHERE date(SUBSTR(CDHDR.UDATE, 1, 10)) <= ?
        )
    GROUP BY
        update_date
    ORDER BY
        update_date;
    '''

    cursor.execute(query, get_user_inputs())
    result = cursor.fetchall()
    return result

# Define the directory path and file names
file_dir = r'C:\Users\vladimir.shpilkin\Desktop\Learning\Python-Projects\process_mining'
data_files = ['EKPO.csv', 'EKKO.csv', 'CDHDR.csv', 'CDPOS.csv']
table_names = ['EKPO', 'EKKO', 'CDHDR', 'CDPOS']

# Load data into SQLite databases
create_file_paths(file_dir, data_files, table_names)

# Attach databases and get a cursor for querying
conn, cursor = attach_databases(file_dir, table_names)

# Perform the query
query_result = perform_query(cursor)

# Output the query result
for row in query_result:
    print(row)

# Close the connection
conn.close()
