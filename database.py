import os
from dotenv import load_dotenv
import pandas as pd

from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, insert, Numeric, Date, Text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

load_dotenv()

funda_listings_table = os.getenv('FUNDA_LISTINGS_TABLE')

# Get database connection info from environment variables
postgres_user = os.getenv('POSTGRES_USER')
postgres_password = os.getenv('POSTGRES_PASSWORD')
postgres_host = os.getenv('POSTGRES_HOST')
postgres_port = os.getenv('POSTGRES_PORT')
postgres_db = os.getenv('POSTGRES_DB')

SQLALCHEMY_DATABASE_URL = (
    f"postgresql://{postgres_user}:"
    f"{postgres_password}@{postgres_host}:{postgres_port}/"
    f"{postgres_db}"
)
# print(SQLALCHEMY_DATABASE_URL)

engine = create_engine(SQLALCHEMY_DATABASE_URL)

# Replace 'your_table_name' with your actual table name and add your table's columns
metadata = MetaData()
# Define the table with the appropriate columns and data types
properties_table = Table(funda_listings_table, metadata,
                         Column('id', Integer, primary_key=True),
                         Column('street_name', String),
                         Column('house_number', Integer),
                         Column('house_number_addition', String),
                         Column('postal_code', String),
                         Column('city_name', String),
                         Column('floor_size_m2', Integer),
                         Column('plot_size_m2', Integer),
                         Column('number_of_bedrooms', Integer),
                         Column('price_per_m2', Numeric),
                         Column('status', String),
                         Column('year_built', Integer),
                         Column('energy_label', String),
                         Column('description', Text),
                         Column('date_listed', Date),
                         Column('date_sold', Date),
                         Column('url', String),
                         Column('asking_price', Numeric)
                        )



# Create the table in the database if it doesn't exist
metadata.create_all(engine)


# Create a configured "Session" class
Session = sessionmaker(bind=engine)


# # Testing the database connection
# with engine.connect() as connection:
#     result_df = pd.read_sql('SELECT * FROM funda_listings', connection)

# # Now you can use result_df which contains the query results
# print(result_df)


# def get_most_recent_listing_url(engine):
#     """This function is meant to read the table in the database and get the most recently scraped url.
#     The while loop pagination uses this to know where to stop"""
#     df = pd.read_sql_table('funda_listings', engine)
#     # record_with_highest_id = df.loc[df['id'].idxmax()]
#     # url_of_record_with_highest_id = record_with_highest_id['url']
#     sorted_df = df.sort_values(by=['date_listed', 'id'], ascending=[False, True]).drop_duplicates(subset='date_listed', keep='first')
#     most_recent_record = sorted_df.iloc[0]
#     url_of_most_recent_record = most_recent_record['url']
    
#     return url_of_most_recent_record

# a = get_most_recent_listing_url(engine)
# print(a)

# df = pd.read_sql_table('funda_listings', engine)
# # record_with_highest_id = df.loc[df['id'].idxmax()]
# # url_of_record_with_highest_id = record_with_highest_id['url']
# sorted_df = df.sort_values(by=['date_listed', 'id'], ascending=[False, True]) #.drop_duplicates(subset='date_listed', keep='first')
# most_recent_record = sorted_df.iloc[0]
# second_most_recent_record = sorted_df.iloc[1]
# url_of_most_recent_record = most_recent_record['url']

# print(most_recent_record['url'])
# print(second_most_recent_record['url'])