# -*- coding: utf-8 -*-
"""
Created on Sat Oct  6 21:41:18 2018

@author: Ramkumar.Ranganathan
"""
import sqlalchemy
import pyodbc
import urllib
from nsepy.history import get_history
from datetime import datetime, timedelta, date
import matplotlib.pyplot as plt
import pandas as pd

connString = "Driver={SQL Server Native Client 11.0};Server=IN2104950W2;Database=NSEData;trusted_connection=yes;"
params = urllib.parse.quote_plus(connString)
engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

sql = '''
SELECT *
  FROM IndexHistory
  where [index name] = 'NIFTY 50'
'''
df = pd.read_sql_query(sql, engine)
ax = plt.gca()

#df.plot(kind='line',x='Date',y='Close',ax=ax)
df.plot(kind='bar',x='Date',y='Close',color='red',ax=ax)
plt.show()

