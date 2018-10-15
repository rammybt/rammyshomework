# =============================================================================
# # -*- coding: utf-8 -*-
# """
# Created on Wed Oct  3 20:50:02 2018
# 
# @author: Ramkumar.Ranganathan
# """
# from datetime import datetime, timedelta, date
# import pyodbc 
# import pandas as pd
# from pathlib import Path
# from zipfile import ZipFile 
#     
# directory_in_str = "C:\Ram K R\Learning\Machine Learning - Udemy\Machine Learning A-Z\Scraping\Bhavcopies"
# conn = pyodbc.connect("Driver={SQL Server Native Client 11.0};Server=IN2104950W2;Database=NEWSDB;trusted_connection=yes;")
# cur = conn.cursor()
# 
# def insertMasterDataDB(sectionMaster):
#     if not sectionMaster:
#         print("List Empty")
#     else:
#         #conn = sqlite3.connect('newsSite.db')
#         #conn = pyodbc.connect("Driver={SQL Server Native Client 11.0};Server=IN2104950W2;Database=NEWSDB;trusted_connection=yes;")
#         cur.executemany("insert into newsMaster(ndate, sectionheader,headline,contentURL) values (?,?,?,?)", sectionMaster)
#         cur.commit()
#         
# def unzipallfiles():
#     cnt=0
#     pathlist = Path(directory_in_str).glob('**/*.zip')
#     for path in pathlist:
#         # because path is object not string
#         cnt = cnt+1
#         path_in_str = str(path)
#         try:
#             with ZipFile(path_in_str, 'r') as zip:
#                 zip.extractall()
#                 print(path_in_str + " :: Completed") 
#         except:
#             print(path_in_str + " :: Not Completed")
#             continue
# 
# pathlist = Path(directory_in_str).glob('**/*.csv')
# for path in pathlist:
#     path_in_str = str(path)
#     print(path_in_str) 
#     a = pd.read_csv(path_in_str) 
#     break
# 
# 
# =============================================================================

import nsepy
import sqlalchemy
import pyodbc
import urllib
from nsepy.history import get_price_list
from datetime import datetime, timedelta, date
import pandas as pd

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)
    

connString = "Driver={SQL Server Native Client 11.0};Server=IN2104950W2;Database=NSEData;trusted_connection=yes;"
params = urllib.parse.quote_plus(connString)
engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

sql = '''
SELECT max(CONVERT(varchar, TIMESTAMP, 23)) as maxdate
FROM 
Bhavcopy
'''
df = pd.read_sql_query(sql, engine)

lastupdateddate = datetime.strptime(df['maxdate'][0].split()[0], "%Y-%m-%d").date()

startdate = lastupdateddate + timedelta(days=1)
enddate = datetime.now().date() + timedelta(days=1)

dtrange = daterange(startdate, enddate)

starttime = datetime.now()
print("Started : ", starttime)

for dt in dtrange:
    yr = dt.year
    mth = dt.month
    dte = dt.day  
    try:
        if(dt.weekday() < 5  ):
            prices = get_price_list(dt=date(yr,mth,dte))
            prices['TIMESTAMP'] = pd.to_datetime(prices['TIMESTAMP'])
            prices.to_sql("bhavcopy", engine, if_exists='append', chunksize=1000)
            print(dt, " - Completed :: ", len(prices.index)," ROWS, ", len(prices.columns), " COLUMNS")
    except Exception as e:
        print(dt, " - Not Completed :: ",e)
        continue
endtime = datetime.now()
print("Finished : ", endtime)
print("Completed in", endtime-starttime)