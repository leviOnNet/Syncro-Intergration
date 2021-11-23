import pandas as pd
from sqlalchemy import create_engine
import mysql.connector

engine    = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
        .format(
            db="Appdb",
            user = "Levi",
            pw="T3c#T3c#"
        )) 

for chunk in pd.read_sql('SELECT user, password FROM user',con=engine,index_col=None,coerce_float=True,params=None,parse_dates=None,chunksize=500):
            if len(chunk)>0:
                chunck_df = pd.DataFrame(chunk)
                sql_user = chunck_df['user'][0]
                sql_password = chunck_df['password'][0]
                print(sql_user)
                print(sql_password)