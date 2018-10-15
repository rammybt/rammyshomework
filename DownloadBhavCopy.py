# -*- coding: utf-8 -*-
"""
Created on Thu Sep 27 17:02:51 2018

@author: Ramkumar.Ranganathan
"""

# imported the requests library 
import requests
from fake_useragent import UserAgent
from datetime import datetime, timedelta, date
import calendar

#globally defing the fake useragent 
ua = UserAgent()
headers = {'User-Agent': str(ua.firefox)}

# create a date range with the given start and end date
def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

# download URL using request lib
def downloadURL(url, filename):    
    #image_url = "https://nseindia.com/content/historical/EQUITIES/2013/JAN/cm01JAN2013bhav.csv.zip"
    #print(ua.firefox)
    r = requests.get(url,headers=headers) # create HTTP response object 
    print (r)
    #"cm01JAN2013bhav.csv.zip"
    with open(filename,'wb') as f: 
        f.write(r.content)
        
startdate = date(2015, 1, 1)
enddate = date(2015,1,31)

dtrange = daterange(startdate, enddate)

for dt in dtrange:
    yr = str(dt.year)
    mth = dt.strftime('%b').upper()
    dte = dt.strftime('%d')
    filename = "cm"+dte+mth+yr+"bhav.csv.zip"
    url = "https://nseindia.com/content/historical/EQUITIES/"+yr+"/"+mth+"/"+filename
    print(url)
    downloadURL(url, filename)
    print("completed")
    