# -*- coding: utf-8 -*-
"""
Created on Wed Dec 27 10:29:18 2017

@author: Ramkumar.Ranganathan
"""
from bs4 import BeautifulSoup
import requests
from datetime import timedelta, date
#import sqlite3
import pyodbc 
from datetime import datetime
import pandas as pd
import base64
import re

parse = "Master"
#parse = "Sections"


def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

def scrapeTheURL(url):
    r = requests.get(url)    
    data = r.text
    soup = BeautifulSoup(data,"lxml")
    return soup    

def parseMaster(date):
    url = "http://www.thehindu.com/archive/print/"+str(date.year)+"/"+str(date.month)+"/"+str(date.day)+"/"
    #url ="http://www.thehindu.com/todays-paper/tp-features/tp-metroplus/takedown-from-tokyo/article19959316.ece"
    
    soup = scrapeTheURL(url)
    
    sectionMaster=[]
    for item in soup.find_all('section', {"id": lambda x: x and x.startswith('section_')}):
        for subitem1 in item.find_all('a', {'class':"section-list-heading"}):
    #        print("Section Header : " + subitem1.text)
            myDate = (str(date),)
            for subitem in item.find_all('li'):
                    sectionData=()
                    sectionData =  sectionData + myDate
                    sectionData = sectionData + (subitem1.text,)
                    sectionData = sectionData + (str(subitem.a.contents),)
                    sectionData = sectionData + (subitem.a.get('href'),)
    #                print (subitem.a.get('href'))
    #                print (subitem.a.contents)
                    sectionMaster.append(sectionData)
    return sectionMaster

def insertMasterDataDB(sectionMaster):
    if not sectionMaster:
        print("List Empty")
    else:
        #conn = sqlite3.connect('newsSite.db')
        #conn = pyodbc.connect("Driver={SQL Server Native Client 11.0};Server=IN2104950W2;Database=NEWSDB;trusted_connection=yes;")
        cur.executemany("insert into newsMaster(ndate, sectionheader,headline,contentURL) values (?,?,?,?)", sectionMaster)
        cur.commit()

def parseSubSection(masid, secUrl):
    secsoup = scrapeTheURL(secUrl)

    SectionTitle = secsoup.select_one('h1.title').text
    SectionName = secsoup.select_one('a.section-name').text
    NewsTimeStamp = secsoup.select_one('.author-container .ksl-time-stamp').text
    NewsUpdateTimeStamp = secsoup.select_one('.author-container .update-time').text
    if secsoup.select_one(".auth-nm"):
        authorname = secsoup.select_one(".auth-nm").text
        if secsoup.select_one(".author-container img")['src']:
            response = requests.get(secsoup.select_one(".author-container img")['src'])
            if response.status_code==200:
                authorimage = base64.b64encode(response.content)
    if secsoup.find("img",{"class":"lead-img"}):
        response = requests.get(secsoup.find("img",{"class":"lead-img"})['data-proxy-image'])
        if response.status_code==200:
            sectionImage = base64.b64encode(response.content)
    if secsoup.select_one('.lead-img-caption p'):
        sectionImageCaption = secsoup.select_one('.lead-img-caption p').text
        if secsoup.select_one('.intro'):
            sectionintroduction = secsoup.select_one('.intro').text
    sectioncontentbody = secsoup.find("div", {"id" : re.compile('content-body*')}).text
    fullHtml = secsoup
    
    sectionDF.append({'newsMasterId':masid,'SectionTitle':SectionTitle,'SectionName':SectionName},ignore_index=True)
    print(sectionDF)
    
if parse == "Master":
    #conn = sqlite3.connect("C:\\Users\\ramkumar.ranganathan\\Desktop\\Machine Learning - Udemy\\Machine Learning A-Z\\Scraping\\newsSite.db")

    conn = pyodbc.connect("Driver={SQL Server Native Client 11.0};Server=IN2104950W2;Database=NEWSDB;trusted_connection=yes;")

    cur = conn.cursor()
    
    cur.execute("SELECT max(ndate) FROM newsMaster")
    #print(cur.fetchone()[0])
    
    res = cur.fetchone() 
    
#    if sorted(cur.fetchone()) == 'None':
#        lastScrapedDate = date(2006, 1, 1)
#    else:
#        lastScrapedDate = cur.fetchone()

    if res[0] is not None:    
        lastScrapedDate = datetime.strptime(str(res[0]), '%Y-%m-%d')
    else:
        lastScrapedDate = datetime(2006, 1, 1)
    
    end_date = date(2018, 10, 13)
        
    dtRange = daterange(datetime.date(lastScrapedDate + timedelta(days=1)),end_date)
    print("Starting from : " + str(lastScrapedDate + timedelta(days=1)))
    
    for dt in dtRange:
       secMaster = parseMaster(dt)
       insertMasterDataDB(secMaster)
       print("Completed for Date : " + str(dt))

elif parse == "Sections":
    print ("Sections")    
    
    conn = pyodbc.connect("Driver={SQL Server Native Client 11.0};Server=IN2104950W2;Database=NEWSDB;trusted_connection=yes;")
    cur = conn.cursor()
    cur.execute("select mas.id, mas.ndate, mas.contentURL from newsMaster mas left outer join SectionContent cont on mas.id = cont.newsMasterId where contenturl is not null order by ndate asc")
    sectionDF = pd.DataFrame()
    while True:
        secRes = cur.fetchmany()
        if secRes == ():
            break
        for key in secRes:           
            parseSubSection(key[0], key[2])
            #print(key[2])

conn.close()        
##################################################################################
#for single_date in daterange(start_date, end_date):
    #print single_date.strftime("%Y-%m-%d")
#    print ("Year : " + str(single_date.year) + " --- Month : " + str(single_date.month) + " --- Date : " + str(single_date.day))

#conn = sqlite3.connect('newsSite.db')
#print ("Opened database successfully");
#
#conn.execute('''CREATE TABLE newsMaster
#         (NDATE TEXT NOT NULL,
#         sectionheader           TEXT    NOT NULL,
#         headline TEXT     NOT NULL,
#         contentURL   TEXT);''')
#print ("Table created successfully");

#print(content)
#[{
#    sectionheading = "National",
#  {
#      newsitem = [{"title" = "abcd",
#                "Content" = "asdlkfjsalkdfjlksadjflksadjflksdaj",
#                "images" = [{"img1":"abcd.jpg",
#                           "img2":"abdkf.jpg"
#                           }]
#                }]
#      }
#    }
# ]
