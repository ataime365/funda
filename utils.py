from lxml import html
import time
from random import randint
import pandas as pd
import re
import os
from dotenv import load_dotenv


load_dotenv()

funda_listings_table = os.getenv('FUNDA_LISTINGS_TABLE')


# User Agent rotation
list_of_user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.3",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.3",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.3",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36 Edg/117.0.2045.4",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36 Edg/112.0.1722.5",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.5",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.3",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.3",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.3",
    "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.14"
]


def split_address(address):
    parts = address.split()
    street_name = []
    house_number = ''
    house_number_addition = []

    # Flag to indicate that the house number has been found
    found_house_number = False

    for part in parts:
        if part.isdigit() and not found_house_number:
            house_number = part # making the first number house_number
            found_house_number = True
        elif found_house_number:
            # All subsequent parts after finding the house number are considered part of the addition
            house_number_addition.append(part)
        else:
            street_name.append(part)
    if 'ouwnr' in address: #Bouwnr #building number
        house_number = address.split("ouwnr.")[1].replace(")", "").strip()

    street_name = ' '.join(street_name)
    house_number_addition = ' '.join(house_number_addition)  # Join any parts found after the house number as the addition
    return street_name, house_number, house_number_addition



def extract_status(response):
    """This function is meant to take the response of a page
    and return the current status"""
    # time.sleep(randint(10, 15)) #remove this after putting proxy
    # time.sleep(randint(2, 4)) #remove this after putting proxy
    tree = html.fromstring(response.content)
    # status = tree.xpath('//dt[contains(text(), "Status")]/following::span[1]')[0]
    status = tree.xpath('//dt[contains(text(), "Status")]/following::span[1]')
    if status:
        status = status[0].text
    elif len(status) ==0:
        status = ''
    current_status = status
    return current_status


def get_most_recent_listing_url(engine):
    """This function is meant to read the table in the database and get the most recently scraped url.
    The while loop pagination uses this to know where to stop"""
    df = pd.read_sql_table(funda_listings_table, engine)
    sorted_df = df.sort_values(by=['date_listed', 'id'], ascending=[False, True])
    most_recent_record = sorted_df.iloc[0]
    url_of_most_recent_record = most_recent_record['url']
    
    return url_of_most_recent_record


def extract_year(text):
    # Define a regular expression pattern to match a sequence of digits
    pattern = re.compile(r'\d+')

    # Search for the pattern in the input text
    match = pattern.search(text)

    # If a match is found, return it
    if match:
        return match.group()
    else:
        # Return None or an appropriate value if no numeric part is found
        return None