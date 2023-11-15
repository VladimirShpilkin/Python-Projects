import sqlite3

ekpo_db_path = 'C:\\Users\\vladimir.shpilkin\\Desktop\\Learning\\Python-Projects\\process_mining\\EKPO.db'
ekko_db_path = 'C:\\Users\\vladimir.shpilkin\\Desktop\\Learning\\Python-Projects\\process_mining\\EKKO.db'
cdhdr_db_path = 'C:\\Users\\vladimir.shpilkin\\Desktop\\Learning\\Python-Projects\\process_mining\\CDHDR.db'
cdpos_db_path = 'C:\\Users\\vladimir.shpilkin\\Desktop\\Learning\\Python-Projects\\process_mining\\CDPOS.db'

conn = sqlite3.connect(':memory:')
cursor = conn.cursor()

# Attach the databases
cursor.execute(f"ATTACH DATABASE '{ekpo_db_path}' AS EKPO")
cursor.execute(f"ATTACH DATABASE '{ekko_db_path}' AS EKKO")
cursor.execute(f"ATTACH DATABASE '{cdhdr_db_path}' AS CDHDR")
cursor.execute(f"ATTACH DATABASE '{cdpos_db_path}' AS CDPOS")

# Get user inputs
client=int(input("Enter the client number: "))
po_number = int(input("Enter PO Number: "))
update_date = input("Enter Update Date (YYYY-MM-DD): ")

# Perform the SELECT query with user inputs
query = '''
    SELECT
        EKPO.MANDT AS client,
        EKPO.EBELN AS po_number,
        EKPO.EBELP AS item_number,
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
        EKPO.MANDT=? AND EKPO.EBELN = ? AND date(SUBSTR(CDHDR.UDATE, 1, 10)) = ?
    GROUP BY
        update_date
    ORDER BY
        update_date;
    '''

cursor.execute(query, (po_number, update_date))
result = cursor.fetchall()

for row in result:
    print(row)

# Close the connection
conn.close()
