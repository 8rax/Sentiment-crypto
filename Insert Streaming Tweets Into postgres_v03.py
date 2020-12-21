#/*******************************************************
#Nom ......... : Insert Streaming Tweets Into postgres_v03.py
#Context ......: Natural language processing and Crypto Prices
#Role .........: Get tweets, apply sentiment analysis and store in DB      
#Auteur ...... : JDO
#Version ..... : V1.1
#Date ........ : 09.12.2020
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
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import psycopg2
from configparser import ConfigParser
import yfinance as yf
import random 
import numpy as np
import re
from datetime import date, timedelta, datetime
import datetime

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

#Import the sentiment analyzer from VADER
analyzer = SentimentIntensityAnalyzer()

#Get Connection details in .ini file for TWITTER
def config_TWITTER(filename='credentials.ini', section='TWITTER'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)
    # get section, default to postgresql
    twit_config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            twit_config[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
    return twit_config

#Get the connection details     
param_twitter=config_TWITTER()


def remove_pattern(input_txt, pattern):
    r = re.findall(pattern, input_txt)
    for i in r:
        input_txt = re.sub(i, '', input_txt)        
    return input_txt

def clean_tweets(tweets):
    #remove twitter Return handles (RT @xxx:)
    tweets = np.vectorize(remove_pattern)(tweets, "RT @[\w]*:") 
    #remove twitter handles (@xxx)
    tweets = np.vectorize(remove_pattern)(tweets, "@[\w]*")
    #remove URL links (httpxxx)
    tweets = np.vectorize(remove_pattern)(tweets, "https?://[A-Za-z0-9./]*")  
    #remove special characters, numbers, punctuations (except for #)
    tweets = np.core.defchararray.replace(tweets, "[^a-zA-Z]", " ")
    
    return tweets


class listener(StreamListener):
    def on_data(self, data):
        try:
            data = json.loads(data)
            print("---------STARTING----------------")
            #print(data)
            #Cleaning the tweets
            if data['text'][:2] != 'RT' and data['user']['followers_count'] > 50 and data['lang']=='en':
                id_tweet=data['id']
                #print("---------PRINT RAW----------------")
                #print(data['id_str'])
                #print(data['text'])
                tweet_list = []
                tweet_list.append(data['text'])
                tweet = clean_tweets(tweet_list)
                #print("---------PRINT CLEANED----------------")
                #print(tweet[0])
                time_ms = int(data['timestamp_ms'])/1000
                time_dt=datetime.datetime.fromtimestamp(time_ms).isoformat()
                vs = analyzer.polarity_scores(tweet)
                sentiment = vs['compound']
                #print("---------PRINT  TIME----------------")
                print(time_dt)
                #print("---------PRINT  SENTIMENT----------------")
                #print( sentiment)
                if sentiment != 0.0:
                    print("Tweet Insertion started")
                    cur.execute("INSERT INTO sent (id_tweet, date, tweet, sentiment) VALUES (%s,%s, %s, %s)",(id_tweet,time_dt, tweet[0], sentiment))                 
                    conn.commit()
                    print("Tweet Inserted")
                else:
                    print("Sentiment unclear")
            else:
                #print(data['text'])
                print("Tweet does not look relevant")
                #print("Language =%s"%data['lang'])
                #print("Follower count =%s"%data['user']['followers_count'])
                #print("RT Status = %s"%data['text'][:2])
        except KeyError as e:
            print(str(e))
        return(True)

    def on_error(self, status):
        print(status)

while True:
    try:
        auth = OAuthHandler(param_twitter['ckey'],param_twitter['csecret'])
        auth.set_access_token(param_twitter['atoken'], param_twitter['asecret'])
        twitterStream = Stream(auth, listener())
        twitterStream.filter(track=["Bitcoin"])
    except Exception as e:
        print(str(e))
        time.sleep(5)

