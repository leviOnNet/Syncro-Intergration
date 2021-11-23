import pymysql
from pandas.core.frame import DataFrame
import requests
import pandas as pd
import os
from sqlalchemy import create_engine



engine    = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
        .format(
            db="syncroapidb",
            user = "Levi",
            pw="T3c#T3c#"
        )) 

df = pd.read_excel('SLA_CLIENTS.xlsx')

try:
        df.to_sql('sla_clients',con=engine,if_exists='append',index=True)
except:
    pass
else:
    print("saved in the table")
print(df)