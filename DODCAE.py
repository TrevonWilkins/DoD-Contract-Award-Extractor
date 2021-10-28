#!/usr/bin/env python

import requests
import re
import pandas as pd
import tabulate
import openpyxl
import numpy as np
import os
import time
import matplotlib.pyplot as plt
from bs4 import BeautifulSoup
from openpyxl import load_workbook
from IPython.display import display_html
from itertools import chain,cycle
from matplotlib.backends.backend_pdf import PdfPages

def program_mode():
    mode = input("Regular(R) or Backdate(B) mode? ")
    mode = mode.lower()
    print()
    return (mode)

while True:
    mode = program_mode()
    if mode.startswith('r'):
        start_time = time.time()
        
        def pull_freshsite():
            url = requests.get("https://www.defense.gov/News/Contracts/")
            url = BeautifulSoup(url.text, "html.parser")
            url = str(url.find('listing-titles-only'))
            findlinks = re.findall(r'http://www.defense.gov/News/Contracts/Contract/Article/[0-9]{7,7}/', (url))
            return findlinks
        
        def regular_mode():
            mode = input("Is this link Fresh(F) or Old(O)? ")
            mode = mode.lower()
            print()
            return (mode)
        
        while True:
            mode = regular_mode()
            if mode.startswith('f'):
                findlinks = pull_freshsite()
                link = findlinks[0]
                link = link.replace("http","https")
                break
            elif mode.startswith('o'):
                link = input("Insert Old Article Link: ")
                break
            else:
                print("Error: Unknown mode. Please use Regular or Backdate, otherwise submit request to program author.")
                break

        def results_collection():
            res = requests.get(link)
            soup2 = BeautifulSoup(res.text, "html.parser")
            results = soup2.find('div', attrs={'class':'body'}).find_all("p") #Article content HTML location
            return results
        results = results_collection()

        def visited_links():
            pd.set_option('display.max_colwidth', None)
            visited_links = [link]
            df_links = pd.DataFrame()
            df_links['visited_links'] = visited_links
            return df_links

        #Paragraph collection
        paragraphs = [str(x) for x in results]

        #Monetary collection
        monetary, organizations, locations = [], [], []
        for x in paragraphs:
            amount = re.findall(r'(\$\d+\,\d{3}\,\d{3}\,?\d{0,3})', (x))
            monetary.append(str(amount[0:1]))
            monetary_dic = {'$':'',',':'','\'':'','[':'',']':''}
            for key, value in monetary_dic.items():
                monetary = ([str.replace(key, value) for str in monetary])
        #Organizations collection.
            org_name = re.findall(r'^(.+?),', (x))
            organizations.append(str(org_name))
            organizations_dic = {'<p>':'','\'':'','[':'',']':'','&amp':'','The':'','Inc':'',';':'','.':'','"':''}
            for key, value in organizations_dic.items():
                organizations = ([str.replace(key, value) for str in organizations])
        #Locations collection.
            loc = re.findall(r'(Alabama|Alaska|Arizona|Arkansas|Australia|California|Colorado|Connecticut|Delaware|Florida|Georgia|Hawaii|Idaho|Illinois|Indiana|Iowa|Kansas|Kentucky|Louisiana|Maine|Maryland|Massachusetts|Michigan|Minnesota|Mississippi|Missouri|Montana|Nebraska|Nevada|New Hampshire|New Jersey|New Mexico|New York|North Carolina|North Dakota|Ohio|Oklahoma|Oregon|Pennsylvania|Puerto Rico|Rhode Island|South Carolina|South Dakota|Tennessee|Texas|Utah|Vermont|Virginia|Washington|West Virginia|Wisconsin|Wyoming/)', (x))
            locations.append(str(loc[0:1]))
            locations_dic = {'\'':'','[':'',']':''}
            for key, value in locations_dic.items():
                locations = ([str.replace(key, value) for str in locations])  

        def organization_funds():
            max_size=2
            df_orgfunds = pd.DataFrame()
            df_orgfunds['Organizations'] = organizations
            df_orgfunds['Monetary'] = monetary
            df_orgfunds['Monetary'].replace('', np.nan, inplace=True)
            df_orgfunds['Organizations'].replace('', np.nan, inplace=True)
            df_orgfunds.dropna(how='any', inplace = True)
            df_orgfunds['Monetary'] = df_orgfunds['Monetary'].astype(int)
            df_orgfunds['Organizations'] = df_orgfunds['Organizations'].str.upper().str.title()
            df_orgfunds['Organizations'] = df_orgfunds['Organizations'].apply(lambda x: ' '.join(x.split(maxsplit=max_size)[:max_size]))
            df_orgfunds.Monetary = np.where( df_orgfunds.Monetary > 7500000, df_orgfunds.Monetary/ 1000000, df_orgfunds.Monetary)
            df_orgfunds = df_orgfunds.groupby('Organizations', as_index=False).agg({'Monetary':sum})
            pd.options.display.float_format = "{:,.1f}".format
            return df_orgfunds

        def location_funds():
            df_locationfunds = pd.DataFrame()
            df_locationfunds['Locations'] = locations
            df_locationfunds['Monetary'] = monetary
            df_locationfunds['Monetary'].replace('', np.nan, inplace=True)
            df_locationfunds['Locations'].replace('', np.nan, inplace=True)
            df_locationfunds.dropna(how='any', inplace = True)
            df_locationfunds['Monetary'] = df_locationfunds['Monetary'].astype(int)
            df_locationfunds.Monetary = np.where(df_locationfunds.Monetary > 7500000, df_locationfunds.Monetary/ 1000000, df_locationfunds.Monetary)
            df_locationfunds = df_locationfunds.groupby('Locations', as_index=False).agg({'Monetary':sum})
            return df_locationfunds

        def display_side_by_side(*args,titles=cycle([''])):
            html_str=''
            for df,title in zip(args, chain(titles,cycle(['</br>'])) ):
                    html_str+='<th style="text-align:center"><td style="vertical-align:top">'
                    html_str+=f'<h2>{title}</h2>'
                    html_str+=df.to_html().replace('table','table style="display:inline"')
                    html_str+='</td></th>'
            display_html(html_str,raw=True)

        def buildxlsx():
            pd.set_option('display.max_colwidth', None)
            df_locationfunds = location_funds()
            df_orgfunds = organization_funds()
            df_links = visited_links()
            dflist = [df_orgfunds, df_locationfunds, df_links]
            writer = pd.ExcelWriter("DefenseContracts_Data.xlsx", engine = "openpyxl")
            for i, df in enumerate (dflist):
                    df.to_excel(writer, sheet_name="Sheet" + str(i+1), index=False)
            writer.save()
            Sheet1, Sheet2, Sheet3 = readcurrentxlsx()

            #Printing Two DataFrames side-by-side in table format
            org_monetary = Sheet1.sort_values(by="Monetary", ascending=False, axis =0, ignore_index=True)
            location_monetary = Sheet2.sort_values(by="Monetary", ascending=False, axis=0, ignore_index=True)
            display_side_by_side(org_monetary.head(10),location_monetary.head(10), titles=['Organizations', 'Locations'])
        
        def readcurrentxlsx():
            Sheet1 = pd.read_excel("DefenseContracts_Data.xlsx", sheet_name="Sheet1", engine="openpyxl")
            Sheet2 = pd.read_excel("DefenseContracts_Data.xlsx",sheet_name="Sheet2", engine="openpyxl")
            Sheet3 = pd.read_excel("DefenseContracts_Data.xlsx",sheet_name="Sheet3", engine="openpyxl")
            return Sheet1, Sheet2, Sheet3

        def buildoncurrentxlsx():
            df_locationfunds, df_orgfunds, df_links = location_funds(), organization_funds(), visited_links()
            pd.set_option('display.max_colwidth', None)
            pd.options.display.float_format = "{:,.1f}".format
            Sheet1, Sheet2, Sheet3 = readcurrentxlsx()
            book = load_workbook("DefenseContracts_Data.xlsx")
            writer = pd.ExcelWriter("DefenseContracts_Data.xlsx", engine='openpyxl') 
            writer.book = book
            writer.sheets = dict((ws.title, ws) for ws in book.worksheets)
            df_orgfunds = df_orgfunds.append(Sheet1)
            df_locationfunds = df_locationfunds.append(Sheet2)
            df_links = df_links.append(Sheet3)
            df_orgfunds = df_orgfunds.groupby('Organizations', as_index=False).agg({'Monetary':sum})
            df_locationfunds = df_locationfunds.groupby('Locations', as_index=False).agg({'Monetary':sum})
            #Appending new contract data.
            dflist = [df_orgfunds, df_locationfunds,df_links]
            for i, df in enumerate (dflist):
                    df.to_excel(writer, sheet_name="Sheet" + str(i+1), index=False)
            writer.save()
            #Chart Creation
            Sheet1, Sheet2, Sheet3 = readcurrentxlsx()  
            Sheet1.head(10).sort_values('Monetary', ascending=False)[['Organizations','Monetary']].plot.bar(style='dict', ylabel='Monetary (in millions)', fontsize=12, figsize=(12,12), x='Organizations', y='Monetary')
            Sheet2.head(10).sort_values('Monetary', ascending=False)[['Locations','Monetary']].plot.bar(style='dict', ylabel='Monetary (in millions)', fontsize=12, figsize=(12,12), x='Locations', y='Monetary')
            #Printing Two DataFrames side-by-side in table format
            org_monetary = Sheet1.sort_values(by="Monetary", ascending=False, axis =0, ignore_index=True)
            location_monetary = Sheet2.sort_values(by="Monetary", ascending=False, axis=0, ignore_index=True)
            display_side_by_side(org_monetary.head(10),location_monetary.head(10), titles=['Organizations', 'Locations'])

        #Prints the Website for easy access
        print("Website Link:", link)
        print()

        if os.path.isfile('./DefenseContracts_Data.xlsx')==False:
            buildxlsx()
        else: 
            Sheet1, Sheet2, Sheet3 = readcurrentxlsx()
            linkcheck = Sheet3['visited_links'].values.tolist()
            if link in linkcheck:
                print("We already analyzed this link. Script is closing.")
                time.sleep(2)
                exit()
            else:
                buildoncurrentxlsx()
        print()       
        print("Regular mode complete")
        print()
        print("Program Runtime: %s seconds" % (time.time() - start_time))
        break
   
    elif mode.startswith('b'):
        start_time = time.time()
        
        def backdate_mode():
            mode = input("Inserting Dates(D) or a Link(L)? ")
            mode = mode.lower()
            print()
            return (mode)
        
        def backdated_links():
            while True:
                mode = backdate_mode()
                if mode.startswith('d'):
                    print()
                    stock_quarters = pd.DataFrame(["Q1: 1JAN – 31MAR","Q2: 1APR – 30JUN","Q3: 1JUL – 30SEP","Q4: 1OCT – 31DEC"])
                    print(stock_quarters)
                    print()
                    backdate_url = f'https://www.defense.gov/News/Contracts/StartDate/{input ("Start Format (YYYY-MM-DD):")}/EndDate/{input ("End Format (YYY-MM-DD):")}/'
                    print()
                    break
                elif mode.startswith('l'):
                    backdate_url = input("Insert Date Range Link: ")
                    print()
                    break
                else:
                    print("Error: Unknown mode. Please use Dates or Link, otherwise submit request to program author.")
                    break
            webpage = requests.get(backdate_url)
            webpage = BeautifulSoup(webpage.text, "html.parser")
            webpage = webpage.find_all('a')
            webpage = (str(webpage))
            page_search,page_links = [], []
            page_search = re.findall(r'http://www.defense.gov/News/Contracts/StartDate/[0-9]{4}-[0-9]{2}-[0-9]{2}/EndDate/[0-9]{4}-[0-9]{2}-[0-9]{2}/[?]Page=[0-9]{1,2}|http://www.defense.gov/News/Contracts/[?]Page=[0-9]{1,2}', (webpage))
            [page_links.append(str) for str in page_search if str not in page_links]
            page_links = ([str.replace("http", "https") for str in page_links])
            page_links = ([str.replace("httpss", "https") for str in page_links])
            page_links.append(backdate_url)
            links, findlinks = [], []
            for x in page_links:
                link = requests.get(x)
                link = BeautifulSoup(link.text, "html.parser")
                link = str(link.find_all('listing-titles-only'))
                findlinks = re.findall(r'http://www.defense.gov/News/Contracts/Contract/Article/[0-9]{7,7}/', (link))
                [links.append(str) for str in findlinks if str not in links]
                links = ([str.replace("http", "https") for str in links])
                links = ([str.replace("httpss", "https") for str in links])
            return links

        def backdate():
            links = backdated_links()
            for x in links:
                def results_collection():
                    res = requests.get(x)
                    soup2 = BeautifulSoup(res.text, "html.parser")
                    results = soup2.find('div', attrs={'class':'body'}).find_all("p") #Article content HTML location
                    return results
                results = results_collection()

                def visited_links():
                    pd.set_option('display.max_colwidth', None)
                    visited_links = [x for x in links]
                    df_links = pd.DataFrame()
                    df_links['visited_links'] = visited_links
                    return df_links

                #Paragraph collection
                paragraphs = [str(x) for x in results]

                #Monetary Amounts of each paragraph. 
                monetary, organizations, locations = [], [], []
                for x in paragraphs:
                    amount = re.findall(r'(\$\d+\,\d{3}\,\d{3}\,?\d{0,3})', (x))
                    monetary.append(str(amount[0:1]))
                    monetary_dic = {'$':'',',':'','\'':'','[':'',']':''}
                    for key, value in monetary_dic.items():
                        monetary = ([str.replace(key, value) for str in monetary])
                #Organizations collection.
                    org_name = re.findall(r'^(.+?),', (x))
                    organizations.append(str(org_name))
                    organizations_dic = {'<p>':'','\'':'','[':'',']':'','&amp':'','The':'','Inc':'',';':'','.':'','"':''}
                    for key, value in organizations_dic.items():
                        organizations = ([str.replace(key, value) for str in organizations])
                #Locations collection.
                    loc = re.findall(r'(Alabama|Alaska|Arizona|Arkansas|Australia|California|Colorado|Connecticut|Delaware|Florida|Georgia|Hawaii|Idaho|Illinois|Indiana|Iowa|Kansas|Kentucky|Louisiana|Maine|Maryland|Massachusetts|Michigan|Minnesota|Mississippi|Missouri|Montana|Nebraska|Nevada|New Hampshire|New Jersey|New Mexico|New York|North Carolina|North Dakota|Ohio|Oklahoma|Oregon|Pennsylvania|Puerto Rico|Rhode Island|South Carolina|South Dakota|Tennessee|Texas|Utah|Vermont|Virginia|Washington|West Virginia|Wisconsin|Wyoming/)', (x))
                    locations.append(str(loc[0:1]))
                    locations_dic = {'\'':'','[':'',']':''}
                    for key, value in locations_dic.items():
                        locations = ([str.replace(key, value) for str in locations])    

                def organization_funds():
                    max_size=2
                    df_orgfunds = pd.DataFrame()
                    df_orgfunds['Organizations'] = organizations
                    df_orgfunds['Monetary'] = monetary
                    df_orgfunds['Monetary'].replace('', np.nan, inplace=True)
                    df_orgfunds['Organizations'].replace('', np.nan, inplace=True)
                    df_orgfunds.dropna(how='any', inplace = True)
                    df_orgfunds['Monetary'] = df_orgfunds['Monetary'].astype(int)
                    df_orgfunds['Organizations'] = df_orgfunds['Organizations'].str.upper().str.title()
                    df_orgfunds['Organizations'] = df_orgfunds['Organizations'].apply(lambda x: ' '.join(x.split(maxsplit=max_size)[:max_size]))
                    df_orgfunds.Monetary = np.where( df_orgfunds.Monetary > 7500000, df_orgfunds.Monetary/ 1000000, df_orgfunds.Monetary)
                    df_orgfunds = df_orgfunds.groupby('Organizations', as_index=False).agg({'Monetary':sum})
                    pd.options.display.float_format = "{:,.1f}".format
                    return df_orgfunds

                def location_funds():
                    df_locationfunds = pd.DataFrame()
                    df_locationfunds['Locations'] = locations
                    df_locationfunds['Monetary'] = monetary
                    df_locationfunds['Monetary'].replace('', np.nan, inplace=True)
                    df_locationfunds['Locations'].replace('', np.nan, inplace=True)
                    df_locationfunds.dropna(how='any', inplace = True)
                    df_locationfunds['Monetary'] = df_locationfunds['Monetary'].astype(int)
                    df_locationfunds.Monetary = np.where(df_locationfunds.Monetary > 7500000, df_locationfunds.Monetary/ 1000000, df_locationfunds.Monetary)
                    df_locationfunds = df_locationfunds.groupby('Locations', as_index=False).agg({'Monetary':sum})
                    return df_locationfunds

                def buildxlsx():
                    pd.set_option('display.max_colwidth', None)
                    df_locationfunds, df_orgfunds, df_links = location_funds(), organization_funds(), visited_links()
                    dflist = [df_orgfunds, df_locationfunds, df_links]
                    writer = pd.ExcelWriter("DefenseContracts_Data_Backdate.xlsx", engine = "openpyxl")
                    for i, df in enumerate (dflist):
                            df.to_excel(writer, sheet_name="Sheet" + str(i+1), index=False)
                    writer.save()

                def readcurrentxlsx():
                    Sheet1 = pd.read_excel("DefenseContracts_Data_Backdate.xlsx", sheet_name="Sheet1", engine="openpyxl")
                    Sheet2 = pd.read_excel("DefenseContracts_Data_Backdate.xlsx",sheet_name="Sheet2", engine="openpyxl")
                    Sheet3 = pd.read_excel("DefenseContracts_Data_Backdate.xlsx",sheet_name="Sheet3", engine="openpyxl")
                    return Sheet1, Sheet2, Sheet3

                def buildoncurrentxlsx():
                    df_locationfunds, df_orgfunds, df_links = location_funds(), organization_funds(), visited_links()
                    pd.set_option('display.max_colwidth', None)
                    pd.options.display.float_format = "{:,.1f}".format
                    Sheet1, Sheet2, Sheet3 = readcurrentxlsx()
                    book = load_workbook("DefenseContracts_Data_Backdate.xlsx")
                    writer = pd.ExcelWriter("DefenseContracts_Data_Backdate.xlsx", engine='openpyxl') 
                    writer.book = book
                    writer.sheets = dict((ws.title, ws) for ws in book.worksheets)
                    df_orgfunds = df_orgfunds.append(Sheet1)
                    df_locationfunds = df_locationfunds.append(Sheet2)
                    df_links = df_links.append(Sheet3)
                    df_links = df_links.drop_duplicates()
                    df_orgfunds = df_orgfunds.groupby('Organizations', as_index=False).agg({'Monetary':sum})
                    df_locationfunds = df_locationfunds.groupby('Locations', as_index=False).agg({'Monetary':sum})
                    #Appending new contract data.
                    dflist = [df_orgfunds, df_locationfunds,df_links]
                    for i, df in enumerate (dflist):
                            df.to_excel(writer, sheet_name="Sheet" + str(i+1), index=False)
                    writer.save()

                if os.path.isfile('./DefenseContracts_Data_Backdate.xlsx')==False:
                    buildxlsx()
                else: 
                    buildoncurrentxlsx()

        backdate()
        print()
        
        def rename_file():
            while True:
                os.path.isfile('./DefenseContracts_Data_Backdate.xlsx')
                rename = input("Rename Files Yes(Y) or No(N)? ")
                rename = rename.lower()
                if rename.startswith('y'):
                    old_file_name = "./DefenseContracts_Data_Backdate.xlsx"
                    print("New file name is?")
                    new_file_name = f'./{input()}.xlsx'
                    os.rename(old_file_name, new_file_name)
                    print("File renamed!")
                    break
                elif rename.startswith('n'):
                    print("Ok. We wont rename.")
                    break
                else:
                    print("Error: Unknown binary answer. Please use yes or no, otherwise submit request to program author...")
                    break
        rename_file()

        print()        
        print("Program Runtime: %s seconds" % (time.time() - start_time))
        print()
        print("All done. Backdate mode complete")
        break

    else:
        print("Error: Unknown mode. Please use Regular or Backdate, otherwise submit request to program author.")
