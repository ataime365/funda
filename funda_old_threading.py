
# First access the database or csv file
# put the records in a pandas dataframe
# use pandas to filter for not Verkocht 
# Then send get requests to the url to check the status of that listing
# if status is different from what is in the pandas df, update the dataframe

import pandas as pd
import requests
import os
import random
from datetime import datetime
from sqlalchemy import create_engine, MetaData, Table, update
from sqlalchemy.orm import Session
from dotenv import load_dotenv
import concurrent.futures

from utils import list_of_user_agents, extract_status
from database import engine


load_dotenv()

scraperapi_apikey = os.getenv('SCRAPERAPI_APIKEY')
funda_listings_table = os.getenv('FUNDA_LISTINGS_TABLE')


today = datetime.now()
# Create a new datetime object with the same date but with the time stripped to 00:00:00
todays_date = datetime(today.year, today.month, today.day)


# df = pd.read_csv('funda1.csv')
df = pd.read_sql_table(funda_listings_table, engine) #database

# Filter out records with status 'Verkocht'
df_filtered = df[df['status'] != 'Verkocht'] #listings that are not Sold
df_filtered = df_filtered.tail(5) #[:20] # Remove this later
print(len(df_filtered), " Number of Rows")

# Reflect the table from the database
metadata = MetaData(bind=engine)
table = Table(funda_listings_table, metadata, autoload_with=engine)


def process_row(row):
    try:
        url = row['url']
        print(url)
        selected_user_agent = random.choice(list_of_user_agents)
        headers = {'User-Agent': selected_user_agent,'Accept-Language': 'en-US,en;q=0.9,de;q=0.8',}

        # response = requests.get(url, headers=headers) #response for each individual page
        payload = {'api_key': scraperapi_apikey, 'url': url } # Use this two lines to use scraperapi
        response = requests.get('https://api.scraperapi.com/', params=payload)
        print(response.status_code, 'status code')
        
        if response.status_code == 200:

            current_status = extract_status(response)  # Assuming extract_status() processes response content
            print(f"current_status: {current_status}")
            # current_status = 'Verkocht' # For testing purpose #Remove

            updates = {}
            if current_status != row['status']:  # Update status
                updates['status'] = current_status

            if current_status == 'Verkocht':  # Update date_sold if applicable
                updates['date_sold'] = todays_date

            return (row['id'], updates)
        else:
            print(f"Failed to fetch data for URL: {url}")
            return None

    except Exception as e:
        print(f"An unexpected error occurred while processing {url}: {e}")
        return None


MAX_THREADS = 5  # Adjust this number based on your system's capabilities and the server's tolerance

with Session(engine) as session, concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
    # Prepare a list of futures
    futures = [executor.submit(process_row, row) for index, row in df_filtered.iterrows()]

    for future in concurrent.futures.as_completed(futures):
        result = future.result()
        if result:
            row_id, updates = result
            if updates:
                stmt = update(table).where(table.c.id == row_id).values(**updates)
                session.execute(stmt)
    
    # Commit the transaction
    session.commit()


