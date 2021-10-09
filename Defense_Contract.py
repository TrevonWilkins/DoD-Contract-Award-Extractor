#!/usr/bin/python3.9

import requests #HTTP GET
import re #Regular expression module
from bs4 import BeautifulSoup #Beautifulsoup
import pandas as pd
import numpy as np
import os

#Pulling a Fresh Site
def fresh_site():
    url = "https://www.defense.gov/News/Contracts/"
    webpage = requests.get(url)
    soup = BeautifulSoup(webpage.text, "html.parser")
    htmlresults = soup.find('listing-titles-only')
    links = (str(htmlresults))
    findlinks = re.findall(r'http://www.defense.gov/News/Contracts/Contract/Article/[0-9]{7,7}/', (links))
    finalhtml = findlinks[0]
    #finalhtml = 'insert something'  #Pulling backdated URLs if needed. This will overrule the previous finalhtml variable.
    return finalhtml
finalhtml = fresh_site()

def results_collection():
    res = requests.get(finalhtml)
    soup2 = BeautifulSoup(res.text, "html.parser")
    results = soup2.find('div', attrs={'class':'body'}).find_all("p") #Article content HTML location
    del results[0] #Gets rid of the <p> style and <strong> tags that we dont need.
    del results[-1]
    return results

results = results_collection()

def format_dataframe():
    max_size=2
    df = pd.DataFrame(org_monetary.items(), columns=['Organizations', 'Monetary'])
    df['Monetary'].replace('', np.nan, inplace=True)
    df['Organizations'].replace('', np.nan, inplace=True)
    df.dropna(how='any', inplace = True)
    df['Monetary'] = df['Monetary'].astype(int)
    df['Organizations'] = df['Organizations'].str.upper().str.title() #Converts all organizations to Camel Case for standardization
    df['Organizations'] = df['Organizations'].apply(lambda x: ' '.join(x.split(maxsplit=max_size)[:max_size]))
    df.Monetary = np.where( df.Monetary > 7500000, df.Monetary/ 1000000, df.Monetary)
    df = df.groupby('Organizations', as_index=False).agg({'Monetary':sum}) ##Adds duplicate values of repeat contracting companies
    pd.options.display.float_format = "{:,.1f}".format ##Setting decimal values to 1 float for cleaner look
    return df

def get_chart(df):
    #Charting the Daily Data Bar and Pie
    df.sort_values('Monetary', ascending=False)[['Organizations','Monetary']].plot.bar(style='dict', ylabel='Monetary (in millions)', fontsize=12, 
       figsize=(12,12), x='Organizations', y='Monetary')
    #df.plot.pie(y='Monetary',figsize=(10, 10),autopct='%1.1f%%', startangle=90)

#Paragraph collection
paragraphs = []
for x in results: 
    paragraphs.append(str(x)) 
    
#Monetary Amounts of each paragraph. 
monetary = []
organizations = []
locations = []
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
    
#Locations collection. Potential employment?
    loc = re.findall(r'(Alabama|Alaska|Arizona|Arkansas|Australia|California|Colorado|Connecticut|Delaware|Florida|Georgia|Hawaii|Idaho|Illinois|Indiana|Iowa|Kansas|Kentucky|Louisiana|Maine|Maryland|Massachusetts|Michigan|Minnesota|Mississippi|Missouri|Montana|Nebraska|Nevada|New Hampshire|New Jersey|New Mexico|New York|North Carolina|North Dakota|Ohio|Oklahoma|Oregon|Pennsylvania|Puerto Rico|Rhode Island|South Carolina|South Dakota|Tennessee|Texas|Utah|Vermont|Virginia|Washington|West Virginia|Wisconsin|Wyoming/)', (x))
    locations.append(str(loc[0:1]))
    locations = list(filter(None, locations))
    locations_dic = {'\'':'','[':'',']':''}
    for key, value in locations_dic.items():
        locations = ([str.replace(key, value) for str in locations])

org_monetary = {organizations[i]: monetary[i] for i in range(len(organizations))} #Merging two lists into a dictionary for the length of dictionary
    
#Prints the Website for easy access
print("Website:", finalhtml)
print() ##Add a space in output

#Cleaning up data if DataContract file does not exist.
def Filenoexist():
    df = format_dataframe()
    df.to_csv("DefenseContracts_Data.csv", encoding='utf-8') #Exporting and creating CSV
    print(df)
    print() ##Add a space in output
    get_chart(df)

#Cleaning up data if DataContract does exist.
def Filedoesexist():
    df = format_dataframe()
    #df['Locations'] = locations
    print(df) #Prints the daily report
    print() ##Add a space in output
    get_chart(df)
    
    df.to_csv('DefenseContracts_Data.csv', mode='a', header=False) #Exporting to existing CSV
    df = pd.read_csv('DefenseContracts_Data.csv')
    df = df.groupby('Organizations', as_index=False).agg({'Monetary':sum}) ##Adds duplicate values of repeat contracting companies
    df.to_csv('DefenseContracts_Data.csv') #Exporting to existing CSV
    print(df) #Prints the aggregated report
    print() ##Add a space in output
    df_location = pd.DataFrame(locations, columns = ['Locations/Development/Hiring'])
    df_location = df_location.value_counts()
    print(df_location)
    get_chart(df)

#If statement that executes either the filenoexist or filedoesexist function in or instead of the presence of the DefenseContracts_Data.csv   
if os.path.isfile('./DefenseContracts_Data.csv')==False:
    Filenoexist()
else: 
    Filedoesexist()
    
#Uninvoked Functions:
## This will be used to merge CSV files whenever(quarterly, semi-quarterly, yearly). We will need to point to the correct folders for CSV retrieval.  
    #def merge_csv():
    #CSV1 = pd.read_csv('DefenseContracts_Data(XXXXXX).csv')
    #CSV2 = pd.read_csv('DefenseContracts_Data(XXXXXX).csv')
    #CSV3 = pd.read_csv('DefenseContracts_Data(XXXXXXX).csv')
    #pd.concat([df, df2]).to_csv('QX_CSV.csv', index=False)
    #QX_CSV = pd.read_csv('QX_CSV.csv')
    #QX_CSV = QX_CSV.groupby('Organizations', as_index=False).agg({'Monetary':sum}) ##Adds duplicate values