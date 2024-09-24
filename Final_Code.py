# Importing the required libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
from datetime import datetime 
import mysql.connector  

# URL and other configurations
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
exchange_rate_csv = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv'
table_attribs = ["Name", "MC_USD_Billion"]
db_name = 'banks'  
table_name = 'largest_banks'
csv_path = './Largest_banks_data.csv'

# Logging function
def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'  # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now()  # get current timestamp 
    timestamp = now.strftime(timestamp_format)
    with open("./code_log.txt", "a") as f:
        f.write(timestamp + ' : ' + message + '\n')

# Extract function
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
            data_dict = {"Name": bank_name, "MC_USD_Billion": float(market_cap)}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
    
    return df

# Transform function
def transform(df, exchange_rate_csv):
    exchange_df = pd.read_csv(exchange_rate_csv)
    exchange_rate = exchange_df.set_index('Currency').to_dict()['Rate']

    # Add columns for MC_GBP_Billion, MC_EUR_Billion, and MC_INR_Billion
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]

    return df

# Load to CSV
def load_to_csv(df, csv_path):
    df.to_csv(csv_path, index=False)

# Load to MySQL database (compatible with phpMyAdmin)
def load_to_db(df, db_name, table_name):
    connection = None  # Initialize connection variable
    try:
        # MySQL connection setup (update host, user, password based on your phpMyAdmin settings)
        connection = mysql.connector.connect(
            host='localhost',       # Your MySQL host (often 'localhost' for phpMyAdmin)
            user='root',            # Your MySQL username
            password='',            # Your MySQL password for phpMyAdmin (provide your actual password here)
            database=db_name        # Your MySQL database in phpMyAdmin
        )

        cursor = connection.cursor()

        # Create table if not exists (adjusted for the structure in phpMyAdmin)
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            Name VARCHAR(255),
            MC_USD_Billion FLOAT,
            MC_GBP_Billion FLOAT,
            MC_EUR_Billion FLOAT,
            MC_INR_Billion FLOAT
        );
        """
        cursor.execute(create_table_query)

        # Insert data into MySQL table
        for _, row in df.iterrows():
            insert_query = f"""
            INSERT INTO {table_name} (Name, MC_USD_Billion, MC_GBP_Billion, MC_EUR_Billion, MC_INR_Billion)
            VALUES (%s, %s, %s, %s, %s);
            """
            cursor.execute(insert_query, tuple(row))
        
        connection.commit()
        print("Data loaded into MySQL database successfully.")

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    
    finally:
        if connection is not None and connection.is_connected():
            cursor.close()
            connection.close()

# Running SQL queries
def run_queries(query_statement):
    try:
        connection = mysql.connector.connect(
            host='localhost',       # Your MySQL host
            user='root',            # Your MySQL username
            password='',            # Your MySQL password for phpMyAdmin
            database=db_name,       # Same database in phpMyAdmin
        )
        
        cursor = connection.cursor()
        cursor.execute(query_statement)
        result = cursor.fetchall()

        for row in result:
            print(row)
    
        return result
    
    except mysql.connector.Error as err:
        print(f"Error: {err}")

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

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

# Load to MySQL database
log_progress('Loading data to SQL database')
load_to_db(df_transformed, db_name, table_name)
log_progress('Data saved to SQL database')

# Running a SQL query to check the loaded data
log_progress('Running SQL query on database')
query = f"SELECT * FROM {table_name} WHERE MC_USD_Billion >= 100"
run_queries(query)
log_progress('SQL query executed')

log_progress('ETL process completed')
