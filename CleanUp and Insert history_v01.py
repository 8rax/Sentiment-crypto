#/*******************************************************
#Nom ......... : CleanUp and Insert History.py
#Context ......: Natural language processing and Crypto Prices
#Role .........: clean up of daily tweets and archiving of sentiment           
#Auteur ...... : JDO
#Version ..... : V1.1
#Date ........ : 26.11.2020
#Language : Python
#Version : 3.7.8
#********************************************************/

#********************************************************/

from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import json
from unidecode import unidecode
import time
import sqlite3 
from configparser import ConfigParser
import yfinance as yf
import random 
import numpy as np
import re
from datetime import date, timedelta, datetime
import datetime
import pandas as pd
import pandas.io.sql as sqlio
import psycopg2



#Get Connection details in .ini file for POSTGRES
def config_POSTGRES(filename='credentials.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)
    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
    return db

#Opening connection to postgres database
conn = None
params = config_POSTGRES()
print('Connecting to the PostgreSQL database...')
conn = psycopg2.connect(**params)       
cur = conn.cursor()     
date_yesterday = date.today() - timedelta(days=1)
date_beforeyesterday = date.today() - timedelta(days=2)
date_Month = date.today() - timedelta(days=30)

#Get all the sentiments from the sent table for the last day
print(date.today())
print(date_beforeyesterday)
dat = pd.read_sql_query("SELECT * FROM sent where sentiment is not NULL and CAST(date AS DATE)<%s and CAST(date AS DATE)>%s ;",conn, params=(date.today(),date_beforeyesterday,))
Number_tweets = len(dat.index)
Average_sentiment = 0

#calculate sum of sentiments
for index, row in dat.iterrows():
    Average_sentiment += float(row['sentiment'])

#calculate the average sentiment
if Number_tweets > 0:
    Average_sentiment=Average_sentiment/Number_tweets

    #Check if there is already a line for yesterday
    cur.execute("SELECT * FROM statistics_daily where day=%s;",(date_yesterday,))
    x=cur.fetchone()

    #insert the aggregated sentiment for yesterday
    if  x is None:
        cur.execute("INSERT INTO statistics_daily (day, tweet_nb, av_sent, google_search, transactions_nb, wallet_nb) VALUES (%s, %s, %s, %s, %s, %s)",(date_yesterday,Number_tweets,Average_sentiment,0,0,0))
        conn.commit()
    else:
        print('Already in the table bro')

    #Delete values older than a month
    cur.execute("DELETE FROM sent where date < %s;",(date_Month,))
    conn.commit()
