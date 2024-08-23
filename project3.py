pip install bs4
import pandas as pd
from bs4 import BeautifulSoup
import requests
import sqlite3
from datetime import datetime

#definitions
url = 'https://www.investing.com/indices/major-indices'

db= 'serv1.db'
table='indexes'
conn = sqlite3.connect(db)
log_file=('log.txt')


#function phase
def extract(url, num): #num = max rows to display. Max = 46
    link = requests.get(url).text
    soup = BeautifulSoup(link, 'html.parser')
    table = soup.find_all('tbody')
    rows = table[0].find_all('tr')
    df = pd.DataFrame(columns=['Index_Name', 'Last_Price', 'High', 'Low', 'Change', 'Change_Percent', 'Date'])
    count = 0
    for row in rows:        
        col = row.find_all('td')
        if count <= num:
            if len(col)!= 0:
                name = col[1].find_all('span', dir='ltr')[0].contents[0]
                last = col[2].find_all('span')[0].contents[0]
                high = col[3].contents[0]
                low = col[4].contents[0]
                change = col[5].contents[0]
                percent = col[6].contents[0]
                time = datetime.now().strftime("%Y/%m/%d")
                         
                df = pd.concat([df,pd.DataFrame([{'Index_Name':name, 'Last_Price': last, 'High':high, 'Low':low, 'Change':change, 'Change_Percent': percent, 'Date':time}])], ignore_index=True)
            count = count + 1
        else:
            break
    return df

def transform(df):
    last = [float("".join(x.split(','))) for x in df['Last_Price']] #Be sure to convert numbers to int or float or SQL commands are broken
    high = [float("".join(x.split(','))) for x in df['High']]
    low = [float("".join(x.split(','))) for x in df['Low']]
    df['Last_Price'] = last
    df['High'] = high
    df['Low'] = low
    return df
    

def save_load(df, table, log):
    #with open('pass.csv', 'a') as f:
        #f.write(str(df))
    df.to_csv('New_tracking.csv', mode='a', index=False)   
    df.to_sql(table, conn, if_exists='append', index=False)
    time = datetime.now().strftime('%Y/%m/%d : %H:%M:%S')
    with open(log, 'a') as f:
        f.write('completed loading' + ' @ ' + time + '\n')
 
def run(sql, connection):
    print(sql)
    output = pd.read_sql(sql, connection)
    print(output)


#execution       
a = extract(url, 46)
transform(a)
save_load(a, table, log_file)


sql = f"select * from {table}"
run(sql, conn)






      