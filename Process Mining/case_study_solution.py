import os
import pandas as pd
import sqlite3

# Define the directory path
file_dir = r'C:\Users\vladimir.shpilkin\Desktop\Learning\Python-Projects\process_mining'


#Load data from a CSV file into an SQLite table
def load_data_to_sqlite(file_path, table_name, connection):
    df = pd.read_csv(file_path)
    df.to_sql(table_name, connection, if_exists='replace', index=False)

data_files = ['EKPO.csv', 'EKKO.csv', 'CDHDR.csv', 'CDPOS.csv']
table_names = ['EKPO', 'EKKO', 'CDHDR', 'CDPOS']

# Load CSV data into respective SQLite tables
for file, table in zip(data_files, table_names):
    file_path = os.path.join(file_dir, file)
    load_data_to_sqlite(file_path, table, sqlite3.connect(table + '.db'))


# New connection to perform SQL query
conn = sqlite3.connect(':memory:')
cursor = conn.cursor()

# Find all .db files in the directory and extract their names
db_files = [db_file for db_file in os.listdir(file_dir) if db_file.endswith('.db')]
db_names = [os.path.splitext(db)[0] for db in db_files]

# Attach the databases
for db_name in db_names:
    cursor.execute(f"ATTACH DATABASE '{db_name}.db' AS {db_name}")


# Get user inputs
client = int(input("Enter the client number: "))
po_number = int(input("Enter PO Number: "))
update_date = input("Enter Update Date (YYYY-MM-DD): ")

# Perform the SELECT query with user inputs
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

cursor.execute(query, (client, po_number, update_date))
result = cursor.fetchall()

for row in result:
    print(row)

# Close the connection
conn.close()
