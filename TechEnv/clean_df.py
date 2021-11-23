from sqlalchemy import create_engine
import pandas as pd
 
 
engine    = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
        .format(
            db="syncroapidb",
            user = "Levi",
            pw="T3c#T3c#"
        )) 
store_chunck = []
for chunk in pd.read_sql('SELECT id FROM tickets',con=engine,index_col=None,coerce_float=True,params=None,parse_dates=None,columns='id',chunksize=200):
  print('sql ids')
  chunck_df = pd.DataFrame(chunk)
  print(chunck_df)
  print(chunck_df.dtypes)

id=[['first','47945762'],['second','47945655'],['third','47945666']]
id_df = pd.DataFrame(id,columns=['count','id'])
id_df['id'] = id_df['id'].astype('int')
print('df before')
print(id_df)
print(id_df.dtypes)

fresh_df = id_df[~id_df['id'].isin(chunck_df['id'])]

print('df after')
print(fresh_df)
print(fresh_df.dtypes)