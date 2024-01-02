# Importing the required libraries
import pandas as pd
import numpy as np
from datetime import datetime
import glob
import xml.etree.ElementTree as ET
import requests
import sqlite3
from bs4 import BeautifulSoup

#Initialization:
url = "https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks"
db_name = "Banks.db"
table_name = "Largest_banks"
csv_path = "/home/project/exchange_rate.csv"
out_csv = "Largest_banks_data.csv"
table_attribs = ["Name", "MC_USD_Billion"]
log_file = "code_log.txt"

def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    time_stamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(time_stamp + ':' + message + '\n')

log_progress("Preliminaries complete. Initiating ETL process")

#Extract:
def extract(url, table_attribs):
    df = pd.DataFrame(columns = table_attribs)
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    count = 0 
    for row in rows:
        if count<10:
            col = row.find_all('td')
            if len(col)!=0:
                name_cap = col[1].text.strip('\n')
                market_cap = float(col[2].contents[0].strip('\n'))
                data_dict = {"Name": name_cap,
                             "MC_USD_Billion": market_cap}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df,df1], ignore_index=True)
                count+=1
        else:
            break
    return df

#Transform:
def transform(df, csv_path):
    dataframe = pd.read_csv(csv_path)
    dict_ex = dataframe.set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion'] = [np.round(x*dict_ex['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*dict_ex['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*dict_ex['INR'],2) for x in df['MC_USD_Billion']] 
    
    print(df['MC_EUR_Billion'][4])
    return df


#Load to CSV:
def load_to_csv(transformed_data, output_path):
    transformed_data.to_csv(output_path)
    print(transformed_data)

#loading to Database:
#Creating a connection to SQLite3:
sql_connection = sqlite3.connect('Banks.db')

def load_to_db(df, conn, table_name):
    df.to_sql(table_name, conn, if_exists = 'replace', index =False)

    return df



#Run queries on a Database:
query_statement_1 = f"SELECT * FROM Largest_banks"
query_statement_2 = f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
query_statement_3 = f"SELECT Name from Largest_banks LIMIT 5"

def run_query(query_statement_1, query_statement_2, query_statement_3, conn):
    query_output_1 = pd.read_sql(query_statement_1, conn)
    query_output_2 = pd.read_sql(query_statement_2, conn)
    query_output_3 = pd.read_sql(query_statement_3, conn)


    print(query_output_1, query_output_2, query_output_3)

run_query(query_statement_1, query_statement_2, query_statement_3, sql_connection)

#Log Progress:

extracted_data = extract(url, table_attribs) 
#print(extracted_data)
log_progress("Data extraction complete. Initiating Transformation process")

transformed_data = transform(extracted_data, csv_path)
pd.set_option('display.max_columns', None) #to display all columns
print(transformed_data)
log_progress("Data transformation complete. Initiating Loading process")

load_to_csv(transformed_data, out_csv)
log_progress("Data saved to CSV file")

log_progress("SQL Connection initiated")

load_to_db(transformed_data, sql_connection, table_name)
log_progress("Data loaded to Database as a table, Executing queries")

log_progress("Process Complete")
sql_connection.close()
log_progress("Server Connection closed")