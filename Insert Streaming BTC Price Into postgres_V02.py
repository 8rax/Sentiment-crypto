#/*******************************************************
#Nom ......... : Insert Streaming BTC Price Into postgres_v02.py
#Context ......: Natural language processing and Crypto Prices
#Role .........: Get BTC prices and insert in DB         
#Auteur ...... : JDO
#Version ..... : V1
#Date ........ : 09.12.2020
#Language : Python
#Version : 3.7.8
#********************************************************/
#********************************************************/

from datetime import date, timedelta, datetime
import time
import json
from unidecode import unidecode
import time
import sqlite3 
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import psycopg2
from configparser import ConfigParser
import yfinance as yf
from pytz import timezone
import pytz

#Dates Variables
date_today = date.today()
date_yesterday = date.today() - timedelta(days=1)
#Get BITCOIN TICKER from yahoo finance
bitcoin = yf.Ticker("BTC-USD")
#Update table with missing daily prices for BITCOIN
COUNTER_INSERT = 0

#Function to get Connection details in .ini file for POSTGRES DB
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

#Get Connection details in .ini file for POSTGRES DB and open connection
conn = None
params = config_POSTGRES()
print('Connecting to the PostgreSQL database...')
conn = psycopg2.connect(**params)       
cur = conn.cursor()     

#---------------------- UPDATE CRYPTO_PRICE_DAY TABLE ----------------------
#if we need all history we can use period = max
#hist_bitcoin_daily = bitcoin.history(period="max")

#If not let's get the last record from the table
cur.execute("SELECT MAX(date) FROM crypto_price_day;")
x=cur.fetchone()

#We query the potential missing data from yahoo finance
hist_bitcoin_daily = bitcoin.history(start=x[0],end=date_yesterday)

#Then we loop over the data retrieved from yahoo finance
for index, row in hist_bitcoin_daily.iterrows():
	ccid='BTC'+'_'+str(index)
	cur.execute("SELECT * FROM crypto_price_day where ccid=%s;",(ccid,))
	x=cur.fetchone()
	#If no record concerning the retrieved row then we insert
	if x is None:     
		cur.execute("INSERT INTO crypto_price_day (ccid, crypto, crypto_name, date, open, high, low, close) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",(ccid,'BTC','BITCOIN',index,row['Open'],row['High'],row['Low'],row['Close']))
		conn.commit()
		COUNTER_INSERT += 1
	else:
		continue 

#Print metrics
print('-------------------------------------')
print('FINISHED CRYPTO_PRICE_DAY UPDATE. INSERTED %s rows'%(COUNTER_INSERT))

#---------------------- UPDATE CRYPTO_PRICE_INTRADAY TABLE ----------------------
#Reinitialize counter
COUNTER_INSERT = 0

#let's get the last record from the table
cur.execute("SELECT MAX(date) FROM crypto_price_intraday;")
x=cur.fetchone()

#We query the potential missing data from yahoo finance
hist_bitcoin_intraday = bitcoin.history(start=x[0],interval="5m")

#Then we loop over the data retrieved from yahoo finance
for index, row in hist_bitcoin_intraday.iterrows():
	ccid='BTC'+'_'+str(index)
	cur.execute("SELECT * FROM crypto_price_intraday where ccid=%s;",(ccid,))
	x=cur.fetchone()
	#If no record we insert into the table
	if x is None:           
		cur.execute("INSERT INTO crypto_price_intraday (ccid, crypto, crypto_name, date, price) VALUES (%s, %s, %s, %s, %s)",(ccid,'BTC','BITCOIN',index,row['Close']))
		conn.commit()
		COUNTER_INSERT += 1
	else:
		continue

#Print metrics
print('-------------------------------------')
print('FINISHED CRYPTO_PRICE_INTRADAY UPDATE. INSERTED %s rows'%(COUNTER_INSERT))

#---------------------- STREAMING UPDATES ----------------------
print('-------------------------------------')
print('ENTERING STREAMING')

while True:
	#now = datetime.now()- timedelta(hours=1)
	now = datetime.now()
	current_time = now.strftime("%H:%M:%S")
	#CRYPTO_PRICE_INTRADAY Streaming update
	hist_bitcoin_intraday = bitcoin.history(start=date_today,interval="5m" )
	ccid='BTC'+'_'+str(hist_bitcoin_intraday.index[-1])
	Date = hist_bitcoin_intraday.index[-1]
	price=hist_bitcoin_intraday.iloc[-1]['Close']
	cur.execute("SELECT * FROM CRYPTO_PRICE_INTRADAY where ccid = %s", (ccid,))
	x=cur.fetchone()
	if x is None:           
		cur.execute("INSERT INTO CRYPTO_PRICE_INTRADAY (ccid, crypto, crypto_name, date, Price) VALUES (%s, %s, %s, %s, %s)",(ccid,'BTC','BITCOIN',Date,price))    
		conn.commit()
		print("%s : Value for BTC INTRADAY inserted PRICE = %s for DATE = %s"%(current_time,price, Date))
	else:
		print("%s : No new BTC INTRADAY price"%(current_time))

	#CRYPTO_PRICE_DAY Streaming update
	hist_bitcoin_daily = bitcoin.history(start=date_yesterday, end=date_yesterday)
	ccid='BTC'+'_'+str(hist_bitcoin_daily.index[-1])
	Date = hist_bitcoin_daily.index[-1]
	Open=hist_bitcoin_daily.iloc[-1]['Open']
	High=hist_bitcoin_daily.iloc[-1]['High']
	Low=hist_bitcoin_daily.iloc[-1]['Low']
	Close=hist_bitcoin_daily.iloc[-1]['Close']

	cur.execute("SELECT * FROM CRYPTO_PRICE_DAY where ccid = %s", (ccid,))
	x=cur.fetchone()
	if x is None:           
		cur.execute("INSERT INTO CRYPTO_PRICE_DAY (ccid, crypto, crypto_name, date, Open, High, Low, Close) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",(ccid,'BTC','BITCOIN',Date,Open, High, Low, Close))    
		conn.commit()
		print("%s : Value for BTC DAY inserted PRICE = %s for DATE = %s"%(current_time,Close, Date))
	else: 
		print("%s : No new BTC DAY closing price"%(current_time))


	time.sleep(120)
