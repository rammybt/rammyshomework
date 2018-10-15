# -*- coding: utf-8 -*-
"""
Created on Thu Feb 15 16:25:34 2018

@author: Ramkumar.Ranganathan
"""

from bs4 import BeautifulSoup
import requests
import base64
import re
import pandas as pd
#from datetime import timedelta, date
#from datetime import datetime

#securl = 'http://www.thehindu.com/todays-paper/tp-features/tp-weekend/colour-chemistry-and-rescues/article22532657.ece'
#securl = 'http://www.thehindu.com/todays-paper/tp-national/tp-andhrapradesh/Waiting-for-munificence-of-lsquoremover-of-obstaclesrsquo/article16526823.ece'
securl = 'http://www.thehindu.com/todays-paper/tp-national/tp-kerala/diabetics-beware-of-hypoglycaemia/article8665035.ece'

def scrapeTheURL(url):
    r = requests.get(url)    
    data = r.text
    soup = BeautifulSoup(data,"lxml")
    return soup    

secsoup = scrapeTheURL(securl)

#print(secsoup.a['section-name'])
print("SECTION NAME:" + secsoup.select('a.section-name')[0].string)
print("SECTION TITLE:" + secsoup.select('h1.title')[0].string)
print("CREATED DATE:" + secsoup.select_one('.author-container .ksl-time-stamp').text)
print("UPDATED DATE:" + secsoup.select_one('.author-container .update-time').text)
if secsoup.select_one('.lead-img-caption p'):
    print("IMAGE CAPTION:" + secsoup.select_one('.lead-img-caption p').text)
if secsoup.select_one('.intro'):
    print("SECTION INTRO:" + secsoup.select_one('.intro').text)
if secsoup.find("img",{"class":"lead-img"})['data-proxy-image']:
    print("IMAGE: "+secsoup.find("img",{"class":"lead-img"})['data-proxy-image'])
response = requests.get(secsoup.find("img",{"class":"lead-img"})['data-proxy-image'])
if response.status_code==200:
    b64response1 = base64.b64encode(response.content)
    print("News Image")
print("CONTENT: "+secsoup.find("div", {"id" : re.compile('content-body*')}).text)
if secsoup.select_one('.intro'):
    print("CONTENT INTRO: "+ secsoup.select_one('.intro').text)
if secsoup.select_one(".auth-nm"):
    print(secsoup.select_one(".auth-nm").text)
if secsoup.select_one(".author-container img")['src']:
    response = requests.get(secsoup.select_one(".author-container img")['src'])
    if response.status_code==200:
        b64response2 = base64.b64encode(response.content)
        print("Content Author image")

newDataFrame = pd.DataFrame({
        
        })
    #print(secSoup)
        # class="section-name"
    # class="title"
    # class="author-container ---> class = "ksl-time-stamp" --> class="update-time"
    # class = "img-container picture"
    # class="lead-img-caption" 
    
    