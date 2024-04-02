import requests
from lxml import html
import time
import os
import pandas as pd
import random
from random import randint
from datetime import datetime
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import insert
import concurrent.futures
from dotenv import load_dotenv

from utils import list_of_user_agents, split_address, get_most_recent_listing_url_and_all_urls, extract_year
from database import engine, properties_table, Session


load_dotenv()

scraperapi_apikey = os.getenv('SCRAPERAPI_APIKEY')
funda_listings_table = os.getenv('FUNDA_LISTINGS_TABLE')
MAX_THREADS = os.getenv('MAX_THREADS')

# Get today's date
today = datetime.now()
# Create a new datetime object with the same date but with the time stripped to 00:00:00
todays_date = datetime(today.year, today.month, today.day)


# This is only for scraping new listings, I need a separate script for checking status of older listings
# Pagination
### To make sure we dont scrape old links
total_new_links = [] 
i = 0

old_link1, all_db_url = get_most_recent_listing_url_and_all_urls(engine)
# old_link1 =  get_most_recent_listing_url_and_all_urls(engine)[0] #"https://www.funda.nl/koop/alkmaar/huis-43438698-wielingenweg-203/" #second_to_last_url_from_database
# old_link1 = "https://www.funda.nl/koop/helvoirt/huis-43438730-broekwal-9/" # use this for fresh database and table
print(old_link1, 'old_link1')
# old_link2 = "https://www.funda.nl/koop/helmond/huis-43437288-kaukasus-36/" # last_url_from_database 

# all_db_url = get_most_recent_listing_url_and_all_urls(engine)[1]
print(len(all_db_url))

found_old_link = False  # Flag to indicate if an old link has been found

# Pagination
while True: # pagination #To know where to stop
    i = i +1
    url = f"https://www.funda.nl/zoeken/koop?selected_area=%5B%22nl%22%5D&sort=%22date_down%22&object_type=%5B%22house%22,%22apartment%22,%22land%22%5D&availability=%5B%22available%22%5D&search_result={i}"

    selected_user_agent = random.choice(list_of_user_agents)
    # print(selected_user_agent)
    headers = {
        'User-Agent': selected_user_agent,
        'Accept-Language': 'en-US,en;q=0.9,de;q=0.8',
    }
    # response = requests.get(url, headers=headers)
    
    payload = {'api_key': scraperapi_apikey , 'url': url } # Use this two lines to use scraperapi
    response = requests.get('https://api.scraperapi.com/', params=payload) #No need for mr to specify headers, scraperapi is the one sending the requests for me
    print(f"we are on page {i}")
    # time.sleep(randint(2, 4))

    # if response.status_code == 200:
    tree = html.fromstring(response.content)
    links = tree.xpath('//div[contains(@class, "min-w-0")]/a')
    All_links = []
    for link in links:
        href = link.get('href')
        if href == old_link1 or i==10: #or href == old_link2 #stop at page 10, incase the old_link fails
            print("We have reached an old link, Stop!!")
            found_old_link = True
            break #break out of the for loop
        All_links.append(href)
    # print(len(All_links), " links per page") #remove this later
    total_new_links.extend(All_links)
        
    if found_old_link:
        break  # Breaks out of the while loop if the flag is set

    
print(len(total_new_links), 'total_new_links_list')


def fetch_and_process_page(link):
    """The aim is to run send each request individually, 
    then use multi-threading and proxy to speed up the process """
    try:
        payload = {'api_key': scraperapi_apikey, 'url': link } # Use this two lines to use scraperapi
        response = requests.get('https://api.scraperapi.com/', params=payload)
        print(link)
        # time.sleep(randint(10, 15)) #remove this later when proxy is installed

        tree = html.fromstring(response.content)
        
        street_name_and_num = tree.xpath('//div[contains(@class,"object-header__details-info")]//h1/span[1]')[0].text
        street_name = split_address(street_name_and_num)[0] #(" ").join(street_name_and_num.split(" ")[:-1])
        try:
            house_number = split_address(street_name_and_num)[1] #street_name_and_num.split(" ")[-1]
        except:
            house_number = None
        house_number_addition = split_address(street_name_and_num)[2]
        post_code_container = tree.xpath('//div[contains(@class,"object-header__details-info")]//h1/span[2]')[0].text.strip()
        postal_code = post_code_container.split(" ")[0].strip() + post_code_container.split(" ")[1].strip() #post_code must be first two
        city_name = (" ").join(post_code_container.split(" ")[2:])  #city name takes the rest, and starts from the third one
        
        try:
            asking_price = tree.xpath('//div[contains(@class ,"object-header__pricing")]/div/strong')[0].text
            asking_price = asking_price.replace("k.k.", "").replace("€", "").strip().replace(".", "").split(" ")[0]
        except:
            asking_price = None
        try:
            price_per_m2 = tree.xpath('//dd[contains(@class ,"object-kenmerken-list__asking-price")]')[0].text
            price_per_m2 = price_per_m2.replace("k.k.", "").replace("€", "").strip().replace(".", "")
        except:
            price_per_m2 = None
        
        floor_size_m2 = tree.xpath('//span[contains(text(), "wonen")]/preceding-sibling::span[1]')
        if floor_size_m2:
            floor_size_m2 = floor_size_m2[0].text.split(" ")[0].replace(",", "").replace(".", "").strip()
        elif len(floor_size_m2) ==0:
            floor_size_m2 = None
        # floor_size_m2
        plot_size_m2 = tree.xpath('//span[contains(text(), "perceel")]/preceding-sibling::span[1]')
        if plot_size_m2:
            plot_size_m2 = plot_size_m2[0].text.split(" ")[0].replace(",", "").replace(".", "").strip()
        elif len(plot_size_m2) ==0:
            plot_size_m2 = None
        # plot_size_m2
        number_of_bedrooms = tree.xpath('//span[contains(text(), "slaapkamer")]/preceding-sibling::span[1]')
        if number_of_bedrooms:
            number_of_bedrooms = number_of_bedrooms[0].text.split(" ")[0]
        elif len(number_of_bedrooms) ==0:
            number_of_bedrooms = None
        energy_label = tree.xpath('//span[contains(@class, "energielabel energielabel")]')
        if energy_label:
            energy_label = energy_label[0].text.strip()
        elif len(energy_label) ==0:
            energy_label = ''
        # energy_label
        status = tree.xpath('//dt[contains(text(), "Status")]/following::span[1]')
        if status:
            status = status[0].text
        elif len(status) ==0:
            status = ''

        year_built = tree.xpath('//dt[contains(text(), "Bouwjaar")]/following::span[1]')
        if year_built:
            year_built = year_built[0].text #The [0] is very important
            year_built = extract_year(year_built)
        elif len(year_built) ==0:
            year_built = None
        url = link
        description_elements = tree.xpath('//div[contains(@class, "object-description-body")]')
        description = ""
        # Ensure that the element exists to avoid IndexError
        if description_elements:
            description = ''.join([text for text in description_elements[0].itertext()])
            description = description.strip()
            # print(description)
        else:
            description = ""
        date_listed = todays_date
        date_sold = None
        data_dict = {"street_name":street_name, "house_number":house_number, "house_number_addition":house_number_addition,
                    "postal_code":postal_code, "city_name":city_name, "asking_price":asking_price, "price_per_m2":price_per_m2,
                    "floor_size_m2":floor_size_m2, "plot_size_m2":plot_size_m2, "number_of_bedrooms":number_of_bedrooms,
                    "energy_label": energy_label, "status": status, "year_built":year_built,"description":description ,
                    "url":url, "date_listed":date_listed, "date_sold":date_sold,}
        return data_dict
    except Exception as e:
        print(f"An unexpected error occurred while processing {link}: {e}")
        return None


# This is where the multi-threading starts
# Set the maximum number of threads
MAX_THREADS = MAX_THREADS
with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
    # Use executor.map to apply the function to all links
    results = executor.map(fetch_and_process_page, total_new_links) #[10:15]
    # Results is an iterator of returned values from fetch_and_process_page
    # Filter out None values in case of any exceptions
    processed_items_li = [result for result in results if result is not None]
    # print(processed_items_li[0:2])

# Now processed_items contains all your scraped and processed data

try:
    df = pd.DataFrame(processed_items_li)
    # Convert date fields to the appropriate format
    df['date_listed'] = df['date_listed'].dt.date #converting it to date for postgres db
    df['date_sold'] = pd.to_datetime(df['date_sold']) # Convert the 'date_sold' column to datetime first
    df['date_sold'] = df['date_sold'].dt.date # Then extract only the date component
    df['date_sold'] = df['date_sold'].apply(lambda x: None if pd.isna(x) else x)
    df['date_listed'] = pd.to_datetime(df['date_listed']).dt.date

    # df.replace('', None, inplace=True)

    data_to_insert = df.to_dict(orient='records') #list of dicts


    # # To enable us catch errors while inserting data #Inserting the rows one by one
    # Insert rows one by one using a session within a context manager
    for index, row_data in enumerate(data_to_insert):
        with Session() as session:  # This begins a new session
            try:
                # Insert each row individually
                if row_data['url'] not in all_db_url: #Filtering out duplicates, if any
                    ins_query = insert(properties_table).values(row_data)
                    session.execute(ins_query)
                    session.commit()  # The transaction is committed here
                else:
                    print(f"This is a duplicate url {row_data['url']}")
            except SQLAlchemyError as e:
                # The session is rolled back automatically if an exception occurs
                print(f"Error inserting row {index}: {e}")
                continue  # Proceed with the next row
            # The session is closed automatically when exiting the with block


    # df.to_sql('funda_listings', engine, if_exists='append', index=False) #saving to local postgres db
    # df.to_csv("funda1.csv", index=False) #saving to csv

except Exception as e:
    print("There is an Error:", e)
    print("There are no new listings at the moment")


