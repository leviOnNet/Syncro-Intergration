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
import json 
from pandas import json_normalize



headers = {
    'accept':'application/json',
    'authorization':'Bearer T0f69b295ed3a93afd-2b6e6d52d64a9104ea1db4e69ab0d348'
}
baseurl = 'https://techtechsupport.syncromsp.com/api/v1/'
endpoint  = 'tickets'
global NO_data, fresh_ticket_df
async def get_tickets():
    print("Going through the tickets")
    def main_request(baseurl,endpoint,x,headers):
        r = requests.get(baseurl + endpoint + f'?page={x}',headers=headers)
        return r.json()
    def get_pages(response):

        return response['meta']['total_pages']

    def parse_json(response):
        charlist =[]
        for item in response['tickets']:
            charlist.append(item)
        return charlist
    
    data = main_request(baseurl=baseurl,endpoint=endpoint,x=1,headers=headers)

    main_tickets = []
    for x in range(1,get_pages(data)+1):


        print(x)
        main_tickets.extend(parse_json(main_request(baseurl,endpoint,x,headers)))
    df_tickets = pd.DataFrame(main_tickets)

    #drop faulty and unused columns AKA cleaning dataframe
    df_tickets.drop(['user'],axis=1,inplace=True)
    comments_df =pd.concat([pd.DataFrame(json_normalize(x)) for x in df_tickets['comments'].dropna()],ignore_index=True,sort=False)
    df_tickets.drop(['comments'],axis=1,inplace=True)
    df_tickets['id'] = df_tickets['id'].astype('int')
    df_tickets.drop(['properties'],axis=1,inplace=True)
    new_df_ticket = df_tickets.rename(columns={'id': 'ticket_id'})
    print(comments_df)
    
    
    

    
    new_tickets = pd.merge(new_df_ticket,comments_df, on="ticket_id")
    new_tickets['body'] = new_tickets['body'].astype('string')
    print(new_tickets)
   

    
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
    for chunk in pd.read_sql('SELECT ticket_id FROM tickets',con=engine,index_col=None,coerce_float=True,params=None,parse_dates=None,columns='id',chunksize=50000):
       
        if len(chunk)>0:
            print('sql ids')
            chunck_df = pd.DataFrame(chunk)
            print(chunck_df)
            print("sql types")
            print(chunck_df.dtypes) 
            #create clean dataframe with no duplicates from the sql database
            fresh_ticket_df = new_tickets[~new_tickets['ticket_id'].isin(chunck_df['ticket_id'])]
            if len(fresh_ticket_df)>0:
                print("new tickets uploaded")
                print(fresh_ticket_df)
            else:
                print("there is no new tickets to upload")
            NO_data = False
        else:
            print("there is no data in the tickets sql table provided")
            NO_data = True
  
    #load data from dataframe to sql
    try:
        
            new_tickets.to_sql('tickets',con=engine,if_exists='replace',index=True)
    except Exception as e:
        print(str(e))
        print("something went wrong with the tickets flow")
    else:
        print("ticktets table updated")
    
async def call_tickets():
    await get_tickets()


asyncio.run(call_tickets())
    



    
    
    
  

