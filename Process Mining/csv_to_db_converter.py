import os
import pandas as pd
import sqlite3

new_directory = r'C:\Users\vladimir.shpilkin\Desktop\Learning\Python-Projects\process_mining'

# Check if the directory exists before changing to it
if os.path.exists(new_directory):
    os.chdir(new_directory)
    print('New directory is', os.getcwd())
else:
    print(f'Directory does not exist: {new_directory}')
    exit()



#load the data files
ekpo_data='EKPO.csv'
ekko_data='EKKO.csv'
cdhdr_data='CDHDR.csv'
cdpos_data='CDPOS.csv'

df_ekpo=pd.read_csv(ekpo_data)
df_ekko=pd.read_csv(ekko_data)
df_cdhdr=pd.read_csv(cdhdr_data)
df_cdpos=pd.read_csv(cdpos_data)

#create/connect to SQLite DB
connection_ekpo=sqlite3.connect('EKPO.db')
connection_ekko=sqlite3.connect('EKKO.db')
connection_cdhdr=sqlite3.connect('CDHDR.db')
connection_cdpos=sqlite3.connect('CDPOS.db')


#load data file to SQLite
df_ekpo.to_sql('EKPO', connection_ekpo, if_exists='replace', index=False)
df_ekko.to_sql('EKKO', connection_ekko, if_exists='replace', index=False)
df_cdhdr.to_sql('CDHDR', connection_cdhdr, if_exists='replace', index=False)
df_cdpos.to_sql('CDPOS', connection_cdpos, if_exists='replace', index=False)

#Close connection
connection_ekpo.close()
connection_ekko.close()
connection_cdhdr.close()
connection_cdpos.close()


print('Conversion to SQLite done')
