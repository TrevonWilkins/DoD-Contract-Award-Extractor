#!/usr/bin/env python3
# DoDCAE (Department of Defense Contract Award Extractor) by Trevon Wilkins
# coding: utf-8

import re
import csv
import time
import hashlib
import requests
import pandas as pd
from tqdm import tqdm
from datetime import date
from bs4 import BeautifulSoup
from difflib import SequenceMatcher
from requests.sessions import Session
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

def dodcae(start_date = str, end_date = str, csvfile = False):
    
    """
    Take a given date range and return Department of Defense
    awards within the given range.
    
    :param start_date: The start interval.
    :type number: str
    
    :param end_date: The end interval.
    :type number: str
    
    :(optional) param csvfile: Output CSV file.
    :type boolean, default = False

    :return: pandas dataframe of awards disbursed
    :rtype: df
    
    Example Commands:
    'df = dodcae(start_date="2021-10-01", end_date="2021-10-06")'
    'Q4_2021 = dodcae(start_date="2021-10-01", end_date="2021-12-31")'
    'Q4_2021 = dodcae(start_date="2021-10-01", end_date="2021-12-31", csvfile=True)'
    """
    
    startTime = time.time()
    #Check start and end date format prior to continuation.
    if re.findall('r[0-9]{4}-[0-9]{2}-[0-9]{2}', start_date) or re.findall('r[0-9]{4}-[0-9]{2}-[0-9]{2}', end_date) == [""]:
        return "Invalid date format. Expected format YYYY-MM-DD"
    backdate_url = f'https://www.defense.gov/News/Contracts/StartDate/{start_date}/EndDate/{end_date}/'
    #Input validation
    if backdate_url == re.findall(r'https://www.defense.gov/News/Contracts/StartDate/[0-9]{4}-[0-9]{2}-[0-9]{2}/EndDate/[0-9]{4}-[0-9]{2}-[0-9]{2}/', (backdate_url))[0]:
        links = [backdate_url]
        
    # Search algorithm that defines amount of pages on defense.gov to traverse
    def link_constant(start_date = start_date, end_date = end_date): 
        #Link_constant algorithm =(delta.days *avg#wks_mon * max#links_wk/avg#day_mon)/(max_#links_pg)
        start_date = [int(x.lstrip('0')) for x in start_date.replace('-',',').split(',')]
        end_date = [int(x.lstrip('0')) for x in end_date.replace('-',',').split(',')]
        start_date, end_date = date(start_date[0], start_date[1], start_date[2]), date(end_date[0], end_date[1], end_date[2])
        return int(((end_date - start_date).days * .715)/10)
    links.extend([f'{backdate_url}?Page={i+1}' for i in range(1,link_constant()+1)])
    
    #Multithread article downloads to speed up code execution.
    def download_link(x:str):
        with requests.Session() as response:
            return BeautifulSoup(response.get(x).text, "html.parser")
    def download_all(urls:list):
        with ThreadPoolExecutor(max_workers=None) as executor:
            return list(executor.map(download_link,links))   
            
    #Pulls article links from each page link, deduplicates the results, and converts links into MD5 hash for database integrity use cases.
    links = {k:hashlib.md5(k.encode('utf-8')).hexdigest() for k in list(set([str for str in re.findall(r'http://www.defense.gov/News/Contracts/Contract/Article/[0-9]{7,7}/', str(download_all(links)))]))}
    
    #Grabs all paragraphs of each article, cleanses a few tags, and returns the results to paragraphs variable.
    def results_collection(x):
        contract_date = re.findall(r'[A-Z]{1,1}[a-z]{2,8}\s[0-9]{1,2},\s[0-9]{4,4}',(str(x.find("meta", property="og:title")["content"]) if str(x.find("meta", property="og:title")["content"]) else "no date given"))[0]
        for match in x.find_all(['span', 'a']):
            match.unwrap()
        for p in x.find_all('p'):
            if 'style' in p.attrs:
                del p.attrs['style']
            elif 'class' in p.attrs:
                del p.attrs['class']
        results = [str(x) for x in x.find('div', attrs={'class':'body'}).find_all("p")]
        return contract_date, results 
    paragraphs = {k[0]: k[1] for k in list(map(results_collection,tqdm(download_all(list(links.keys())))))}
    
    #Appends respective contract dates to each paragraph for database granularity.
    def para_aggregation(paragraphs = paragraphs, links = links, new_list = [], x = 0):
        if x >= len(paragraphs):
            return new_list
        else:
            new_list.extend(["".join([item, "".join([list(paragraphs.items())[x][0], list(links.items())[x][0], " ", list(links.items())[x][1]])]) for item in list(paragraphs.values())[x]])
        return para_aggregation(new_list = new_list, x = x+1)
        
    try: 
        #Link to CSV to ticker referencing dictionary.
        url = 'https://raw.githubusercontent.com/TrevonWilkins/DoD-Contract-Award-Extractor/main/Stock%20Dictionary.csv' 
        res = requests.get(url)
        global stocklist, stocklist_list, stocklist_values
        stocklist = dict(csv.reader(list(map(lambda x: x.decode('utf-8'), res.content.splitlines()))))
        stocklist_list, stocklist_values = stocklist.items(), stocklist.values()
    except Exception:
        pass
    #Takes in an organizations name as it appears on defense.gov and tries to guess the stock ticker against the stock dictionary list provided. 
    #The likeness percentage is used to return similarities greater than 85% in order to increase guess quality if the guess is below this threshold the function will return "Ticker Not Found'
    def ticker(a, likeness_percentage = .85, measurement=[0.0,""]):
        try:
            global stocklist, stocklist_values, stocklist_list
            a = a.replace("The", '').lstrip()
            a = a.replace("&amp", '&')
            check = a[0:3]
            res = [idx for idx in stocklist_values if idx.lower().startswith(check.lower())]
            for x in res:
                if x.startswith(("General", "Black", "United", "US", "West", "Parker", "North", "Johns", "Federal", "Huntington", "Science")):
                    b,y = ' '.join(a.split(maxsplit=2)[:2]), ' '.join(x.split(maxsplit=2)[:2])
                else:
                    b,y =  a.split()[0], x.split()[0]
                cost = float(SequenceMatcher(None,b,y).ratio())
                if cost > measurement[0]:
                    measurement = [float(cost),x]
            if measurement[0] > likeness_percentage:   
                for key, value in stocklist_list:
                     if measurement[1] == value:
                        return key
            else:
                return "Ticker Not Found"
        except Exception:
            return "Ticker Not Found"
            
    #Regex search function to build a single table for exportation.
    global regex_run #set as global variable so our multiprocessing function can see it
    def regex_run(x):
        #Processing Dictionaries
        month_translation = {'January':[1,'Q1'],'February':[2,'Q1'],'March':[3,'Q1'],'April':[4,'Q2'],'May':[5,'Q2'],'June':[6,'Q2'],'July':[7,'Q3'],'August':[8,'Q3'],'September':[9,'Q3'],'October':[10,'Q4'],'November':[11,'Q4'],'December':[12,'Q4']}
        organizations_dic = {'<p>':'','\'':'','[':'',']':'','&amp':'','The':'','Inc':'',';':'','.':'','"':''}
        locations_dic = {'\'':'','[':'',']':''}
        monetary_dic = {'$':'',',':'','\'':'','[':'',']':''}
        #Collect [Monetary, Organizations, Location, Month, Day, Year, Quarter, Approximate Ticker, Link, and MD5 Hash Data] database fields. 
        monetary = re.findall(r'(\$\d+\,\d{3}\,\d{3}\,?\d{0,3})', (x))[0:1]
        organizations = re.findall(r'^(.+?),', (x))
        locations = re.findall(r'(Alabama|Alaska|Arizona|Arkansas|Australia|California|Colorado|Connecticut|Delaware|Florida|Georgia|Hawaii|Idaho|Illinois|Indiana|Iowa|Kansas|Kentucky|Louisiana|Maine|Maryland|Massachusetts|Michigan|Minnesota|Mississippi|Missouri|Montana|Nebraska|Nevada|New Hampshire|New Jersey|New Mexico|New York|North Carolina|North Dakota|Ohio|Oklahoma|Oregon|Pennsylvania|Puerto Rico|Rhode Island|South Carolina|South Dakota|Tennessee|Texas|Utah|Vermont|Virginia|Washington|West Virginia|Wisconsin|Wyoming/)', (x))[0:1]
        month, day, year = re.findall(r'[A-Z]{1,1}[a-z]{2,8}\s[0-9]{1,2},\s[0-9]{4,4}', (x))[-1].replace(',', "").split()
        quarter, month = month_translation.get(month)[1], month_translation.get(month)[0]
        link = re.findall(r'http://www.defense.gov/News/Contracts/Contract/Article/[0-9]{7,7}/', (x))[0]
        link_hash = re.findall(r'[0-9a-fA-F]{32}', (x))[0]
        #Cleansing 
        for key, value in locations_dic.items():
            locations = "".join([str.replace(key, value) for str in locations])  
        for key, value in organizations_dic.items():
            organizations = "".join([str.replace(key, value) for str in organizations])
        for key, value in monetary_dic.items():
            monetary = "".join([str.replace(key, value) for str in monetary])
        return organizations, monetary, locations, month, day, year, quarter, ticker(organizations), link, link_hash
        
    with ProcessPoolExecutor(max_workers=None) as executor:
        data = list(executor.map(regex_run, para_aggregation()))
    data = list(filter(lambda tuple_spaces: '' not in tuple_spaces, data)) #Rids empty tuples

    #Cleanse Data and Export to CSV.
    pd.set_option('display.max_colwidth', None)
    df = pd.DataFrame(data, columns =['Organizations', 'Monetary', 'Location', 'Month', 'Day', 'Year', 'Quarter', 'Approximate Ticker', 'Link', 'Link MD5 Hash'])
    df[["Monetary", "Month", "Day", "Year"]] = df[["Monetary", "Month", "Day", "Year"]].apply(pd.to_numeric)
    df['Organizations'] = df['Organizations'].apply(lambda x: ' '.join(x.split(maxsplit=3)[:3]))
    
    if csvfile == True:
        df.to_csv(f'DoDCAE_{start_date}_{end_date}.csv', index=False)

    executionTime = (time.time() - startTime)
    print(); print('Execution time in seconds: ' + str(executionTime))
    return df
