#!/usr/bin/env python3
# coding: utf-8
import requests
import re
import pandas as pd
import time
import hashlib
from bs4 import BeautifulSoup
from tqdm import tqdm
from requests.sessions import Session
from concurrent.futures import ThreadPoolExecutor
from threading import Thread,local
from datetime import date

def dodcae(start_date = str, end_date = str, csv = False):


    """
    Take a given date range and return Department of Defense
    awards within the given range.
    
    :param start_date: The start interval.
    :type number: str
    
    :param end_date: The end interval.
    :type number: str

    :return: pandas dataframe of awards disbursed
    :rtype: df
    Example Command: 'Q4 = dodcae.dodcae(start_date="2021-10-01", end_date="2021-12-31")'
    """
    
    #Check start and end date format prior to continuation.
    if re.findall('r[0-9]{4}-[0-9]{2}-[0-9]{2}', start_date) or re.findall('r[0-9]{4}-[0-9]{2}-[0-9]{2}', end_date) == [""]:
        return "Invalid date format. Expected format YYYY-MM-DD"
        
    links = []
    thread_local = local()
    backdate_url = f'https://www.defense.gov/News/Contracts/StartDate/{start_date}/EndDate/{end_date}/'
    backdate_urlcheck = re.findall(r'https://www.defense.gov/News/Contracts/StartDate/[0-9]{4}-[0-9]{2}-[0-9]{2}/EndDate/[0-9]{4}-[0-9]{2}-[0-9]{2}/', (backdate_url))

    startTime = time.time()

    #Input validation
    if backdate_url == backdate_urlcheck[0]:
        backdate_url = backdate_urlcheck[0]
        links = [backdate_url]
        
    # This function is a custom algortihm that defines amount of pages on defense.gov to search
    def link_constant(start_date = start_date, end_date = end_date): 
        #Link_constant =(delta.days *avg#wks_mon * max#links_wk/avg#day_mon)/(max_#links_pg)
        start_date = [int(x.lstrip('0')) for x in start_date.replace('-',',').split(',')]
        end_date = [int(x.lstrip('0')) for x in end_date.replace('-',',').split(',')]
        start_date = date(start_date[0], start_date[1], start_date[2])
        end_date = date(end_date[0], end_date[1], end_date[2])
        delta = end_date - start_date
        link_constant = int ((delta.days * .715)/10)
        return link_constant
    links.extend([f'{backdate_url}?Page={i+1}' for i in range(1,link_constant())])
    
    #This function takes all pages and searches for individual article links
    def grab_page_links(links,new_list=[], x=0):
        if x == len(links):
            return new_list
        else:
            with requests.get(links[x]) as response:
                search = BeautifulSoup(response.text, "html.parser")
                response.close()
            search = str(search.find_all('listing-titles-only'))
            search_links = re.findall(r'http://www.defense.gov/News/Contracts/Contract/Article/[0-9]{7,7}/', (search))
            [new_list.append(str) for str in search_links if str not in new_list]
        return grab_page_links(links, new_list = new_list, x=x+1)
    links = grab_page_links(links)
    
    #Converts all links gathered into a md5 hash that can be later used for database integrity purposes. Will need to throw in a salt to strengthen.
    def link_hash(x):
        md5 = hashlib.md5(x.encode('utf-8')).hexdigest()
        return x, md5
    link_hashes = list(map(link_hash,links))
    
    #The below get_session, download_link, and download_all functions multithread our article downloads to speed up code execution.
    def get_session() -> Session:
        if not hasattr(thread_local,'session'):
            thread_local.session = requests.Session()
        return thread_local.session
    def download_link(x:str):
        requests = get_session()
        with requests.get(x) as response:
            results = BeautifulSoup(response.text, "html.parser")
            response.close()
        return results
    def download_all(urls:list) -> None:
        with ThreadPoolExecutor(max_workers=10) as executor:
            return list(executor.map(download_link,links))   
    links = tqdm(download_all(links))
    
    #This function grabs all paragraphs of each article and returns the results to a variable (paragraphs).
    def results_collection(x):
        for match in x.find_all('span'):
            match.unwrap()
        for match in x.find_all('a'):
            match.unwrap()
        for p in x.find_all('p'):
            if 'style' in p.attrs:
                del p.attrs['style']
        for p in x.find_all('p'):
            if 'class' in p.attrs:
                del p.attrs['class']
        results = x.find('div', attrs={'class':'body'}).find_all("p") #Article contents location
        return results
    paragraphs = list(map(results_collection, links))

    #Creates list of how many paragraphs exists from each link that we use to pass in our para aggregation function.
    def gather_length(paragraphs, count = [], x = 0):
        if x == len(paragraphs):
            return count
        else:
            count.append(len(paragraphs[x]))
        return gather_length(paragraphs, count = count, x = x+1)
    count = gather_length(paragraphs)

    #Flattens our nested list of list into one single list.
    paragraphs = [str(item) for sublist in paragraphs for item in sublist]

    #Creates list of dates we encounter as we sift through each link. Used for appension to EACH paragraph.
    def contract_date(x):
        contract_date = str(x.find("meta", property="og:title")["content"]) if str(x.find("meta", property="og:title")["content"]) else "no date given"
        contract_date = re.findall(r'[A-Z]{1,1}[a-z]{2,8}\s[0-9]{1,2},\s[0-9]{4,4}',(contract_date))[0]
        return contract_date
    contract_date = list(map(contract_date, links))

    #The para_aggregation appends respective contract dates to each paragraph for database granularity.
    def para_aggregation(paragraphs, count, contract_date, link_hashes, new_list = [], x = 0, start = 0):
        
        if x >= len(count):
            return new_list
        else:
            end = sum(count[0:x]) + sum(count[x:(x+1)])
            joint_string = "".join([contract_date[x], link_hashes[x][0], " ", link_hashes[x][1]])
            contents = ["".join([item, joint_string]) for item in paragraphs[start:end]]
            new_list.extend(contents)
        return para_aggregation(paragraphs, count, contract_date, link_hashes, new_list = new_list, x = x+1, start = end)
    paragraphs = para_aggregation(paragraphs, count, contract_date,link_hashes)

    #Regex search function to build a singular table for exportation.
    def regex_run(x):
        monetary, organizations, locations = [], [], []
        
        #Collect Monetary Amounts. 
        amount = re.findall(r'(\$\d+\,\d{3}\,\d{3}\,?\d{0,3})', (x))
        monetary.append(str(amount[0:1]))
        monetary_dic = {'$':'',',':'','\'':'','[':'',']':''}
        for key, value in monetary_dic.items():
            monetary = ([str.replace(key, value) for str in monetary])
            
        #Collect Organizations.
        org_name = re.findall(r'^(.+?),', (x))
        organizations.append(str(org_name))
        organizations_dic = {'<p>':'','\'':'','[':'',']':'','&amp':'','The':'','Inc':'',';':'','.':'','"':''}
        for key, value in organizations_dic.items():
            organizations = ([str.replace(key, value) for str in organizations])
        
        #Collect Location Data.
        loc = re.findall(r'(Alabama|Alaska|Arizona|Arkansas|Australia|California|Colorado|Connecticut|Delaware|Florida|Georgia|Hawaii|Idaho|Illinois|Indiana|Iowa|Kansas|Kentucky|Louisiana|Maine|Maryland|Massachusetts|Michigan|Minnesota|Mississippi|Missouri|Montana|Nebraska|Nevada|New Hampshire|New Jersey|New Mexico|New York|North Carolina|North Dakota|Ohio|Oklahoma|Oregon|Pennsylvania|Puerto Rico|Rhode Island|South Carolina|South Dakota|Tennessee|Texas|Utah|Vermont|Virginia|Washington|West Virginia|Wisconsin|Wyoming/)', (x))
        locations.append(str(loc[0:1]))
        locations_dic = {'\'':'','[':'',']':''}
        for key, value in locations_dic.items():
            locations = ([str.replace(key, value) for str in locations])  
            
        #Month, Date, Year collection.
        contract_date = re.findall(r'[A-Z]{1,1}[a-z]{2,8}\s[0-9]{1,2},\s[0-9]{4,4}', (x))
        contract_date = contract_date[-1].replace(',', "").split()
        month, day, year = contract_date
        month_translation = {'January':[1,'Q1'],'February':[2,'Q1'],'March':[3,'Q1'],'April':[4,'Q2'],'May':[5,'Q2'],'June':[6,'Q2'],'July':[7,'Q3'],'August':[8,'Q3'],'September':[9,'Q3'],'October':[10,'Q4'],'November':[11,'Q4'],'December':[12,'Q4']}
        quarter, month = month_translation.get(month)[1], month_translation.get(month)[0]
        
        #Collect Source Link.
        link = re.findall(r'http://www.defense.gov/News/Contracts/Contract/Article/[0-9]{7,7}/', (x))
        link_hash = re.findall(r'[0-9a-fA-F]{32}', (x))
        
        return str(organizations[0]), str(monetary[0]), str(locations[0]), month, day, year, quarter, str(link[0]), str(link_hash[0])
    data = list(map(regex_run, paragraphs))
    data = list(filter(lambda tuple_spaces: '' not in tuple_spaces, data)) #Rids empty tuples

    #Cleanse Data and Export to CSV.
    pd.set_option('display.max_colwidth', None)
    df = pd.DataFrame(data, columns =['Organizations', 'Monetary', 'Location', 'Month', 'Day', 'Year', 'Quarter', 'Link', 'Link MD5 Hash'])
    df.Monetary, df.Month, df.Day, df.Year = df.Monetary.astype(int), df.Month.astype(int), df.Day.astype(int), df.Year.astype(int)
    org_name_format=3
    df['Organizations'] = df['Organizations'].apply(lambda x: ' '.join(x.split(maxsplit=org_name_format)[:org_name_format]))
    if csv == True:
            df.to_csv(f'DoDCAE_{start_date}_{end_date}.csv', index=False)

    executionTime = (time.time() - startTime)
    print(); print('Execution time in seconds: ' + str(executionTime))
    return df
