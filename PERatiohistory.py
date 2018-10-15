# -*- coding: utf-8 -*-
"""
Created on Sat Oct  6 22:27:37 2018

@author: Ramkumar.Ranganathan
"""

# -*- coding: utf-8 -*-
"""
Created on Sat Oct  6 18:57:17 2018

@author: Ramkumar.Ranganathan
"""

import nsepy
import sqlalchemy
import pyodbc
import urllib
from nsepy import get_index_pe_history
from datetime import datetime, timedelta, date

connString = "Driver={SQL Server Native Client 11.0};Server=IN2104950W2;Database=NSEData;trusted_connection=yes;"
params = urllib.parse.quote_plus(connString)
engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

indexnames =["CNX NIFTY",	"NIFTY 50",	"NIFTY NEXT 50",	"NIFTY100 LIQ 15",	"NIFTY MID LIQ 15",	"NIFTY 100",	"NIFTY 200",	"NIFTY 500",	"NIFTY MIDCAP 150",	"NIFTY MIDCAP 50",	"NIFTY FULL MIDCAP 100",	"NIFTY MIDCAP 100",	"NIFTY SMALLCAP 250",	"NIFTY SMALLCAP 50",	"NIFTY FULL SMALLCAP 100",	"NIFTY SMLCAP 100",	"NIFTY LargeMidcap 250",	"NIFTY MIDSMALLCAP 400",	"NIFTY AUTO",	"NIFTY BANK",	"NIFTY FIN SERVICE",	"NIFTY FMCG",	"NIFTY IT",	"NIFTY MEDIA",	"NIFTY METAL",	"NIFTY PHARMA",	"NIFTY PVT BANK",	"NIFTY PSU BANK",	"NIFTY REALTY",	"NIFTY COMMODITIES",	"NIFTY CONSUMPTION",	"NIFTY CPSE",	"NIFTY ENERGY",	"NIFTY100 ESG",	"NIFTY100 Enhanced ESG",	"NIFTY INFRA",	"NIFTY MNC",	"NIFTY PSE",	"NIFTY SME EMERGE",	"NIFTY SERV SECTOR",	"NIFTY SHARIAH 25",	"NIFTY50 SHARIAH",	"NIFTY500 SHARIAH",	"NIFTY ADITYA BIRLA GROUP",	"NIFTY MAHINDRA GROUP",	"NIFTY TATA GROUP",	"NIFTY TATA GROUP 25% CAP",	"NIFTY100 LIQ 15",	"NIFTY MID LIQ 15",	"NIFTY ALPHA LOW-VOLATILITY 30",	"NIFTY QUALITY LOW-VOLATILITY 30",	"NIFTY ALPHA QUALITY LOW-VOLATILITY 30",	"NIFTY ALPHA QUALITY VALUE LOW-VOLATILITY 30",	"NIFTY50 EQL WGT",	"NIFTY100 EQL WGT",	"NIFTY100 LOWVOL30",	"NIFTY50 USD",	"NIFTY50 DIV POINT",	"NIFTY DIV OPPS 50",	"NIFTY ALPHA 50",	"NIFTY 50 ARBITRAGE",	"NIFTY 50 FUTURES INDEX",	"NIFTY 50 FUTURES TR INDEX",	"NIFTY HIGH BETA 50",	"NIFTY LOW VOLATILITY 50",	"NIFTY200 QUALITY 30",	"NIFTY100 Quality 30",	"NIFTY50 VALUE 20",	"NIFTY GROWSECT 15",	"NIFTY50 TR 2X LEV",	"NIFTY50 PR 2X LEV",	"NIFTY50 TR 1X INV",	"NIFTY50 PR 1X INV",	"NIFTY GS COMPSITE",	"NIFTY GS 4 8YR",	"NIFTY GS 8 13YR",	"NIFTY GS 10YR",	"NIFTY GS 10YR CLN",	"NIFTY GS 11 15YR",	"NIFTY GS 15YRPLUS",	"NIFTY 10 YEAR SDL INDEX",	"NIFTY AAA CORPORATE BOND",	"NIFTY AAA ULTRA SHORT-TERM CORPORATE BOND",	"NIFTY AAA SHORT-TERM CORPORATE BOND",	"NIFTY AAA MEDIUM-TERM CORPORATE BOND",	"NIFTY AAA LONG-TERM CORPORATE BOND",	"NIFTY AAA ULTRA LONG-TERM CORPORATE BOND",	"Nifty 1D Rate Index"]

sql = '''
SELECT max(CONVERT(varchar, [Date], 23)) as maxdate
FROM 
IndexPEHistory
'''
df = pd.read_sql_query(sql, engine)

lastupdateddate = datetime.strptime(df['maxdate'][0].split()[0], "%Y-%m-%d").date()

startdate = lastupdateddate + timedelta(days=1)
enddate = datetime.now().date() + timedelta(days=1)

print("Last Updated Date = ", lastupdateddate)

for idx in indexnames:
    try:
        indexpevals  = get_index_pe_history(symbol=idx,
                                    start=startdate, 
                                    end=enddate)
        indexpevals["Index Name"] = idx
        indexpevals.to_sql("IndexPEHistory", engine, if_exists='append', chunksize=1000)
        print(idx," -- COMPLETED")
    except Exception as e:
        print(idx," -- NOT COMPLETED")
        print(e)
