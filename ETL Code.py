# Code for ETL operations on Country-GDP data

# Importing the required libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
exchange_rate_csv = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv'  # Ensure you have this locally
table_attribs = ["Name", "MC_USD_Billion"]
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = './Largest_banks_data.csv'

def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

def extract(url, table_attribs):
    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = soup.find_all('tbody')
    rows = tables[0].find_all('tr')
    
    
    for row in rows:
        if row.find('td') is not None:
            col = row.find_all('td')
            bank_name = col[1].find_all('a')[1]['title']
            market_cap = col[2].contents[0][:-1]
            data_dict = {"Name": bank_name, "MC_USD_Billion":float(market_cap)}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)    
                
    return df


df = extract(url, table_attribs)
print(df)


def transform(df, exchange_rate_csv):
    exchange_df = pd.read_csv(exchange_rate_csv)
    
    exchange_rate = exchange_df.set_index('Currency').to_dict()['Rate']
    print(exchange_rate)
    # Add the columns for MC_GBP_Billion, MC_EUR_Billion, and MC_INR_Billion
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]

    return df

df_transformed = transform(df, exchange_rate_csv)

# Print the transformed dataframe
print(df_transformed)


def load_to_csv(df, csv_path):
    df.to_csv(csv_path, index=False)

load_to_csv(df_transformed, csv_path)

def load_to_db(df, db_name, table_name):
    sql_connection = sqlite3.connect(db_name)
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

    sql_connection.close()

load_to_db(df_transformed, db_name, table_name)

def run_queries(query_statement, sql_connection):
    print(f"Executing Query: {query_statement}")
    
    query_output = pd.read_sql_query(query_statement, sql_connection)
    
    print("Query Output:")
    print(query_output)

    return query_output

sql_connection= sqlite3.connect(db_name)
query_statement_1 = "SELECT * FROM Largest_banks"
run_queries(query_statement_1, sql_connection)

query_statement_2 = "SELECT AVG(MC_USD_Billion) FROM Largest_banks"
run_queries(query_statement_2, sql_connection)

query_statement_3 = "SELECT Name FROM Largest_banks LIMIT 5"
run_queries(query_statement_3, sql_connection)

sql_connection.close()


'''Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

# ETL Process
log_progress('Starting ETL process')

# Extract
log_progress('Extracting data')
df = extract(url, table_attribs)
print(df)  # Print extracted data for verification
log_progress('Data extraction complete')

# Transform
log_progress('Transforming data')
df_transformed = transform(df, exchange_rate_csv)
print(df_transformed)  # Print transformed data for verification
log_progress('Data transformation complete')

# Load to CSV
log_progress('Loading data to CSV')
load_to_csv(df_transformed, csv_path)
log_progress('Data saved to CSV')

# Load to DB
log_progress('Loading data to SQL database')
load_to_db(df_transformed, db_name, table_name)
log_progress('Data saved to SQL database')

# Run a query
log_progress('Running SQL query on database')
with sqlite3.connect(db_name) as sql_connection:
    query = f"SELECT * FROM {table_name} WHERE MC_USD_Billion >= 100"
    run_queries(query, sql_connection)
log_progress('SQL query executed')

log_progress('ETL process completed')