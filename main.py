import os

from numpy import append
os.environ['KIVY_GL_BACKEND'] = 'angle_sdl2'

from kivy.uix.boxlayout import BoxLayout
from kivy.uix.screenmanager import ScreenManager, Screen
import kivy
from kivy.app import App
from kivy.uix.label import Label
from kivymd.app import MDApp
from kivy.lang import Builder
import pymysql
from pandas.core.frame import DataFrame
import requests
import pandas as pd
from sqlalchemy import create_engine
import mysql.connector
import MySQLdb


kivy.require('2.0.0')
def sqlconn():
    conn = mysql.connector.connect(
        host = "localhost",
        user = "root",
        password = "T3c#T3c#",
        database = "Appdb"
    )

    cur = conn.cursor
    return cur



engine    = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
        .format(
            db="Appdb",
            user = "Levi",
            pw="T3c#T3c#"
        )) 

class ScreenOne(Screen):
    pass
class ScreenTwo(Screen):
    pass

screen_manager = ScreenManager()
screen_manager.add_widget(ScreenOne(name = "screen_one"))
screen_manager.add_widget(ScreenTwo(name = "screen_two"))

class ContentNavigationDrawer(BoxLayout):
    pass
#Defining a class
class MainApp(MDApp):
        
    #Funtion returning the root widget
    def build(self):
        self.theme_cls.theme_style = "Dark"
        self.theme_cls.primary_palette = "Teal"

        return Builder.load_file('landing_view.kv')
    def logger(self):
       
        username = self.root.ids.user.text
        password = self.root.ids.password.text
        for chunk in pd.read_sql('SELECT user, password FROM user',con=engine,index_col=None,coerce_float=True,params=None,parse_dates=None,chunksize=500):
            if len(chunk)>0:
                chunck_df = pd.DataFrame(chunk)
                sql_user = chunck_df['user'][0]
                sql_password = chunck_df['password'][0]
        if sql_user==username and sql_password == password:
            self.root.ids.welcome_label.text = "Welcome"
            
        else:
            self.root.ids.welcome_label.text = "Wrong credentials"
        


    def clear(self):
        self.root.ids.welcome_label.text = "Welcome to TechTech"
        self.root.ids.user.text = ""
        self.root.ids.password.text = ""
    

#initialize class
MainApp().run()