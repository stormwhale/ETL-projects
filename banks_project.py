pip install bs4
from bs4 import BeautifulSoup
import sqlite3
import numpy as np
from datetime import datetime
import requests
import pandas as pd

url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'

exchange = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv'
ex_rate= pd.read_csv(exchange)['Rate']
csv_path='./Largest_banks_data.csv'

db = 'Banks.db'
table='Largest_banks'
log_file='code_log.txt'

def extract(url):
    resp = requests.get(url).text
    soup = BeautifulSoup(resp, 'html.parser')
    table1= soup.find_all('tbody')[0]
    rows = table1.find_all('tr')
   
    df = pd.DataFrame(columns=["Bank_Name", "MC_USD_Billion"])
    
    for row in rows:
        col = row.find_all('td')
        if len(col)!= 0:
            name = col[1].find_all('a')[1].contents[0]
            mc = float(col[2].contents[0].rstrip()) #.rstrip('\n') can get rid of the whitespace/ n 
            df=pd.concat([df, pd.DataFrame([{"Bank_Name":name, "MC_USD_Billion":mc}])], ignore_index=True)
    return df
# to transform
def transform(df, ex):
    ex = pd.read_csv(exchange)['Rate']
    
    EUR = round((ex[0]*df[['MC_USD_Billion']]),2)
    GBP = round((ex[1]*df[['MC_USD_Billion']]),2)
    INR = round((ex[2]*df[['MC_USD_Billion']]),2)
    dis= {'MC_EUR_Billion ':EUR, 'MC_GBP_Billion':GBP, 'MC_INR_Billion':INR}
    df=df.assign(**dis)
               
    return df

def log(msg,log):
    form = datetime.now().strftime('%Y/%m/%d:%H:%M%S')
    with open(log,'a') as f:
        f.write(msg + " @ " + form +'\n')

def load_to_csv(df, a):
    df.to_csv(a)

def load_to_db(df,conn,statement):
    df.to_sql(table, conn, if_exists='replace',index=False)
    
def run_query(statement):
    print(statement)
    output= pd.read_sql(statement, conn)
    print(output)




log('Preliminaries complete. Initiating ETL process', log_file)
a = extract(url)
a
log('Data extraction complete. Initiating Transformation process', log_file)
new_data = transform(a, ex_rate)
print(new_data)
log('Data transformation complete. Initiating Loading process', log_file)
load_to_csv(new_data, csv_path)
log('Data saved to CSV file', log_file)


conn = sqlite3.connect('db')
log('SQL Connection initiated', log_file)
load_to_db(new_data, conn, sql)
log('Data loaded to Database as a table, Executing queries', log_file)

sql = f"SELECT Bank_Name FROM {table}"

run_query(sql)
log('Process Complete', log_file)

conn.close()
log('Server Connection closed', log_file)



            
    