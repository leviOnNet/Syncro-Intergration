from numpy import append
import pymysql
from pandas.core.frame import DataFrame
import requests
import pandas as pd
import os
from sqlalchemy import create_engine
import schedule
import time
import asyncio


headers = {
    'accept':'application/json',
    'authorization':'Bearer T0f69b295ed3a93afd-2b6e6d52d64a9104ea1db4e69ab0d348'
}
baseurl = 'https://techtechsupport.syncromsp.com/api/v1/'
endpoint  = 'products'
global NO_data, fresh_product_df
async def get_products():
    print("Going through the products")
    def main_request(baseurl,endpoint,x,headers):
        r = requests.get(baseurl + endpoint + f'?page={x}',headers=headers)
        return r.json()
    def get_pages(response):

        return response['meta']['total_pages']

    def parse_json(response):
        charlist =[]
        for item in response['products']:
            charlist.append(item)
        return charlist
    
    data = main_request(baseurl=baseurl,endpoint=endpoint,x=1,headers=headers)

    main_products = []
    for x in range(1,5):

        print(x)
        main_products.extend(parse_json(main_request(baseurl,endpoint,x,headers)))
    df_products = pd.DataFrame(main_products)
    
    df_products['photos'] = df_products['photos'].astype('string') 
    df_products['photos'] = df_products['photos'].str.strip('[]')  
    df_products['vendor_ids'] = df_products['vendor_ids'].astype('string')
    df_products['vendor_ids'] = df_products['vendor_ids'].str.strip('[]')
    df_products['location_quantities'] = df_products['location_quantities'].astype('string')
    df_products['location_quantities'] = df_products['location_quantities'].str.strip('[]')
    df_products['id'] = df_products['id'].astype('string')
    
    df_products.drop(['location_quantities'],axis=1,inplace=True)
    df_products.drop(['photos'],axis=1,inplace=True)
    df_products.drop(['notes'],axis=1,inplace=True)
    df_products.drop(['desired_stock_level'],axis=1,inplace=True)
    df_products.drop(['tax_rate_id'],axis=1,inplace=True)
    df_products.drop(['physical_location'],axis=1,inplace=True)
    df_products.drop(['serialized'],axis=1,inplace=True)
    df_products.drop(['long_description'],axis=1,inplace=True)
    df_products.drop(['price_wholesale'],axis=1,inplace=True)
    df_products.drop(['qb_item_id'],axis=1,inplace=True)
    df_products.drop(['warranty_template_id'],axis=1,inplace=True)
    df_products.drop(['discount_percent'],axis=1,inplace=True)


    engine    = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
        .format(
            db="syncroapidb",
            user = "Levi",
            pw="T3c#T3c#"
        )) 

    NO_data = True
    #get the recent ids from the sql database
    for chunk in pd.read_sql('SELECT id FROM products',con=engine,index_col=None,coerce_float=True,params=None,parse_dates=None,columns='id',chunksize=50000):
       
        if len(chunk)>0:
            print('sql ids')
            chunck_df = pd.DataFrame(chunk)
            print(chunck_df)
            print("sql types")
            print(chunck_df.dtypes) 
            #create clean dataframe with no duplicates from the sql database
            fresh_product_df = df_products[~df_products['id'].isin(chunck_df['id'])]
            if len(fresh_product_df)>0:
                print("new products uploaded")
                print(fresh_product_df)
            else:
                print("there is no new products to upload")
            NO_data = False
        else:
            print("there is no data in the products sql table provided")
            NO_data = True

    #load data from dataframe to sql        
    try:
        if NO_data == False:
            fresh_product_df.to_sql('products',con=engine,if_exists='append',index=True)
        else:
            df_products.to_sql('products',con=engine,if_exists='replace',index=True)
    except:
        print("something went wrong with the products table")
    else:
        print("prodcuts table updated")

    
async def call_products():
    await get_products()
    

