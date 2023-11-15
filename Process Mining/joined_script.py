import os
import pandas as pd

new_directory = r'C:\Users\vladimir.shpilkin\Desktop\Learning\Python-Projects\process_mining'
os.chdir(new_directory)

ekko = 'EKKO.csv'
ekpo = 'EKPO.csv'
cdpos = 'CDPOS.csv'
cdhdr = 'CDHDR.csv'

ekko_df = pd.read_csv(ekko)
ekpo_df = pd.read_csv(ekpo)
cdpos_df = pd.read_csv(cdpos)
cdhdr_df = pd.read_csv(cdhdr)

value1 = input('MANDT')
value2 = input('EBELN')

ekpo_ekko_joined = ekpo_df.set_index(['MANDT', 'EBELN']).join(
    ekko_df.set_index(['MANDT', 'EBELN']), how='left', rsuffix='BURKS'
).reset_index()


#ekpo_ekko_joined.to_csv('joined.csv')


filtered_joined = ekpo_ekko_joined[(ekpo_ekko_joined['MANDT']==value1)&(ekpo_ekko_joined['EBELN']==value2)]






