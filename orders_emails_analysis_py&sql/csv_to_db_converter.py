import os

new_directory = r'C:\Users\vladimir.shpilkin\Desktop\Learning\Python-Projects\orders_emails_analysis_py&sql'

# Check if the directory exists before changing to it
if os.path.exists(new_directory):
    os.chdir(new_directory)
    print('New directory is', os.getcwd())
else:
    print(f'Directory does not exist: {new_directory}')
    exit()

#import the libraries
import pandas as pd
import sqlite3

#load the data files
data_emails='emails_data.csv'
data_orders='orders_data.csv'

df_emails=pd.read_csv(data_emails)
df_orders=pd.read_csv(data_orders)

#create/connect to SQLite DB
connection_emails=sqlite3.connect('emails.db')
connection_orders=sqlite3.connect('orders.db')

#load data file to SQLite
df_emails.to_sql(data_emails, connection_emails, if_exists='replace', index=False)
df_orders.to_sql(data_orders, connection_orders, if_exists='replace', index=False)

#Close connection
connection_emails.close()
connection_orders.close()
print('Conversion to SQLite done')