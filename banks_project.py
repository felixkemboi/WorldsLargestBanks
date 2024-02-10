import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup 
from datetime import datetime 

log_file = "code_log.txt" 
target_file = "largest_banks_data.csv" 
url = "https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks"
rates = "./exchange_rate.csv"
conn = sqlite3.connect('Banks.db')
table_name = 'Largest_banks'


def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ',' + message + '\n') 

log_progress("ETL Job Started")

log_progress("Extract phase Started") 

def extract():

    df = pd.DataFrame(columns=["Name","MC_USD_Billion"])
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')

    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            try:
                bank_name = col[1].text.strip() 
                market_cap = float(col[2].text.strip())  
                data_dict = {"Name": bank_name, "MC_USD_Billion": market_cap}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)
            except ValueError:
                print('skipping due to invalid market cap')
    return df

log_progress("Extract phase Ended") 

# Log the beginning of the Transformation process 
log_progress("Transform phase Started") 

def transform(df, csv_path): 
    
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''

    exchange_rates = pd.read_csv(csv_path)    
    df["MC_EUR_Billion"] = df['MC_USD_Billion']*exchange_rates.loc[exchange_rates["Currency"] == "EUR","Rate"].values[0]
    df["MC_GBP_Billion"] = df['MC_USD_Billion']*exchange_rates.loc[exchange_rates["Currency"] == "GBP","Rate"].values[0]
    df["MC_INR_Billion"] = df['MC_USD_Billion']*exchange_rates.loc[exchange_rates["Currency"] == "INR","Rate"].values[0]
    df["MC_EUR_Billion"] = df["MC_EUR_Billion"].round(2)
    df["MC_GBP_Billion"] = df["MC_GBP_Billion"].round(2)
    df["MC_INR_Billion"] = df["MC_INR_Billion"].round(2)
    print(df['MC_EUR_Billion'][4])
    return df

# Log the completion of the Transformation process 
log_progress("Transform phase Ended") 

# Log the beginning of the Loading process 
log_progress("Load phase Started") 

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
   

    df.to_sql(table_name, sql_connection, if_exists = 'replace', index =False)
    print('Table is ready')

def run_query(conn):
    ''' This function runs the query on the database table and
        prints the output on the terminal. Function returns nothing. '''

    query_statement = "SELECT * FROM Largest_banks"
    query_statement2 = "SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
    query_statement3 = "SELECT Name from Largest_banks LIMIT 5"
    query_output = pd.read_sql(query_statement, conn)
    query_output2 = pd.read_sql(query_statement2, conn)
    query_output3 = pd.read_sql(query_statement3, conn)
    print(query_output)
    print(query_output2)
    print(query_output3)

# Log the completion of the Loading process 
log_progress("Load phase Ended") 

# Log the completion of the ETL process 
log_progress("ETL Job Ended") 

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''
main = extract()
transformer = transform(main, rates)
load_to_csv(transformer, target_file)
load_to_db(transformer, conn, table_name)
#print(main)
run_query(conn)
