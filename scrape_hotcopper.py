# Import modules
import pandas as pd
import numpy as np
from selenium import webdriver
import selenium
import bs4 as bs
from bs4 import BeautifulSoup
import requests
import os
import warnings
import time
import re
import datetime as dt
import asyncio
import aiohttp
from urllib.request import Request, urlopen
import urllib.request
import matplotlib.pyplot as plt
from multiprocessing import Pool
from multiprocessing import Process
from multiprocessing import Queue
import concurrent.futures
from dotenv import load_dotenv
from sqlalchemy import create_engine
import psycopg2
from scrape_asx_tickers import *

# Load environment variables
load_dotenv()

# Creds for PostgreSQL connection
sql_username=os.getenv("sql_username")
sql_pwd=os.getenv("sql_pwd")

# Create a connection to the database
engine = create_engine(f"postgresql://{sql_username}:{sql_pwd}@localhost:5432/hot_copper_db")

# Set ticker list
#ticker_list = ['CXO','AVZ']
#ticker_list = ['QIP','RDG','FLC','CRR','HZR','POD','IPG']
ticker_list = get_asx_tickers()

# Write tickers data to DB
ticker_df = pd.DataFrame(ticker_list)
ticker_df.to_sql('hc_ticker_list', con=engine, if_exists='append')


# Collect count
stock_count = 0

# Initiate for loop for all tickers
for ticker in ticker_list:

    # F string printed for each ticker
    print(f"Getting data for {ticker}.")

    # For loop for range, indicating number of pages to loop through within the stock forum
    for count in range(1, 6):

        # Create dynamic web page url
        discussion_string_page = (
            f"https://hotcopper.com.au/asx/{ticker}/discussion/page-{count}"
        )

        # Create an empty list for HREF links
        links = []

        # Open driver
        driver = webdriver.Chrome()

        try:

            # Open new page
            driver.get(discussion_string_page)

            # Wait for website to be properly opened
            driver.implicitly_wait(5)

            # Use beautiful soup to obtain all page source on curent page
            soup = BeautifulSoup(driver.page_source, "lxml")

            # Find all tables on curent page
            hc_tables = soup.find_all("table")

            # Find all href links in the website
            for link in soup.findAll("a"):
                links.append(link.get("href"))

            # Filter href links for individual posts using list comprehension
            filtered_links = [x for x in links if x != None and "post_id=" in x]

            # Create a dataframe to join onto the primary dataframe
            list_df = pd.DataFrame({"HREF_Link": filtered_links})
            list_df = list_df.apply(lambda x: "https://hotcopper.com.au" + x)
            # Read in data using pandas
            hc_comp = pd.read_html(str(hc_tables))

            # Obtain table data
            hc_comp = hc_comp[0]

            # Drop NA values
            hc_comp = hc_comp.dropna()

            # Remove unwanted columns
            hc_comp = hc_comp.drop(
                columns=[
                    "Forum",
                    "View",
                    "Comments Created with Sketch.",
                    "Views Created with Sketch.",
                ],
            )

            # Rename columns
            hc_comp.columns = ["Ticker", "Subject", "Poster", "Likes", "Date"]

            # Replace date values with today's date if a time value is within date column
            hc_comp.loc[
                (hc_comp["Date"].str.contains(":")), "Date"
            ] = dt.datetime.now().strftime("%d/%m/%y")

            # Reset index
            hc_comp = hc_comp.reset_index(drop=True)

            # Join href links onto the dataframe
            hc_comp = pd.concat([hc_comp, list_df], axis=1)

            # Obtain number of counts in ticker
            hc_comp["Ticker_Filter"] = hc_comp["Ticker"].apply(lambda x: len(x))

            # Filter out for tickers not equal to count of 3
            hc_comp = hc_comp[hc_comp["Ticker_Filter"] == 3]

            # Conditional for initial dataframe
            if count == 1:

                # Compile summary data
                hc_stock_sum = hc_comp

                # Wait for website to be properly opened
                driver.implicitly_wait(10)

            # If not initial dataframe, concatenate data
            else:

                # Concatenate
                hc_stock_sum = pd.concat([hc_stock_sum, hc_comp])

                # Wait for website to be properly opened
                driver.implicitly_wait(10)

            # Append count total
            count += 1

            # Add in sleep timer
            time.sleep(5)

        # Raise exception and flag error with stock
        except Exception:

            # Print error
            print(f"Error identified with {ticker}. Moving to next ticker.")

            # Continue through loop
            continue
            
    # Write compiled summary data to DB
    hc_stock_sum.to_sql('hc_stock_sum', con=engine, if_exists='append')
    
    try:
        # Convert date to datetime
        hc_stock_sum["Date"] = pd.to_datetime(hc_stock_sum["Date"], format="%d/%m/%y")

        # Capture daily comment and like count
        primary_data["Daily_Comments"] = primary_data.groupby(["Date", "Ticker"])[
            "Ticker"
        ].transform("count")
        primary_data["Daily_Likes"] = primary_data.groupby(["Date", "Ticker"])[
            "Likes"
        ].transform("sum")

        # Create new column to determine if news source or individual poster
        hc_stock_sum["Classifier"] = hc_stock_sum["Poster"].apply(
            lambda x: "ASX News" if x == "ASX News" else "Individual"
        )

        # Conditions to append primary dataframe for all stocks
        if stock_count == 0:

            # Set initial datafram at first stock
            primary_data = hc_stock_sum

        # If not the first stock
        elif stock_count > 0:

            # Append the dataframe
            primary_data = pd.concat([primary_data, hc_stock_sum])

        # Add to count
        stock_count += 1

    except Exception:

        continue