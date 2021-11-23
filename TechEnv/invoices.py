from logging import Logger
from numpy import append
from pandas.core.series import Series
import pymysql
from pandas.core.frame import DataFrame
import requests
from sqlalchemy.dialects.mysql import LONGTEXT
import pandas as pd
import os
from sqlalchemy import create_engine
import sqlalchemy as db
import schedule
import time
import asyncio



headers = {
    'accept':'application/json',
    'authorization':'Bearer T0f69b295ed3a93afd-2b6e6d52d64a9104ea1db4e69ab0d348'
}
baseurl = 'https://techtechsupport.syncromsp.com/api/v1/'
endpoint  = 'invoices'
global NO_data, fresh_ticket_df
async def get_invoices():
    print("Going through the invoices")
    def main_request(baseurl,endpoint,x,headers):
        r = requests.get(baseurl + endpoint + f'?page={x}',headers=headers)
        return r.json()
    def get_pages(response):

        return response['meta']['total_pages']

    def parse_json(response):
        charlist =[]
        for item in response['invoices']:
            charlist.append(item)
        return charlist
    
    data = main_request(baseurl=baseurl,endpoint=endpoint,x=1,headers=headers)

    main_invoices = []
    for x in range(1,get_pages(data)+1):

        print(x)
        main_invoices.extend(parse_json(main_request(baseurl,endpoint,x,headers)))
    df_invoices = pd.DataFrame(main_invoices)

    #drop faulty and unused columns AKA cleaning dataframe
    
    df_invoices['id'] = df_invoices['id'].astype('int')

    
    #create a connection to the database
    engine    = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
        .format(
            db="syncroapidb",
            user = "Levi",
            pw="T3c#T3c#"
        )) 

    #bool used to indicate if there is data in the database
    NO_data = True
    
    #get the recent ids from the sql database
    for chunk in pd.read_sql('SELECT id FROM invoices',con=engine,index_col=None,coerce_float=True,params=None,parse_dates=None,columns='id',chunksize=50000):
       
        if len(chunk)>0:
            print('sql ids')
            chunck_df = pd.DataFrame(chunk)
            print(chunck_df)
            print("sql types")
            print(chunck_df.dtypes) 
            #create clean dataframe with no duplicates from the sql database
            fresh_ticket_df = df_invoices[~df_invoices['id'].isin(chunck_df['id'])]
            if len(fresh_ticket_df)>0:
                print("new invoices uploaded")
                print(fresh_ticket_df)
            else:
                print("there is no new tickeks to upload")
            NO_data = False
        else:
            print("there is no data in the invoices sql table provided")
            NO_data = True
  
    #load data from dataframe to sql
    try:
        if NO_data == False:
            fresh_ticket_df.to_sql('invoices',con=engine,if_exists='append',index=True)
        else:
            df_invoices.to_sql('invoices',con=engine,if_exists='replace',index=True)
    except Exception as e:
        print(str(e))
        print("something went wrong with the invoices flow")
    else:
        print("invoces table updated")
    
async def call_invoices():

    await get_invoices()

asyncio.run(call_invoices())



    
    



    
    
    
  

