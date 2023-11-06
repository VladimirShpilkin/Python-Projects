import sqlite3
con=sqlite3.connect('market_stream.db')
cursor=con.cursor()
command="SELECT * FROM Streaming"
cursor.execute(command)

rows=cursor.fetchall()

for row in rows:
    print(row)
con.close()
