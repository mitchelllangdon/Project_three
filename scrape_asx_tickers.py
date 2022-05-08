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
from urllib.request import Request, urlopen
import urllib.request
import matplotlib.pyplot as plt

plt.style.use("ggplot")
#%reload_ext nb_black


def get_asx_tickers():
    # Establish target websites
    hotcopper = "https://hotcopper.com.au/"
    nabtrade = "https://www.nabtrade.com.au/investor/home"
    listed_companies = "https://www.marketindex.com.au/asx-listed-companies"

    # Ignore warnings
    warnings.filterwarnings("ignore")

    # ChromeDriver for Selenium
    driver = webdriver.Chrome()

    # Target website for Selenium to obtain ASX listed data from
    driver.get(listed_companies)

    # Give time for website to load
    driver.implicitly_wait(5)

    # Use beautiful soup to obtain all page source on curent page
    soup = BeautifulSoup(driver.page_source, "lxml")

    # Find all tables on curent page
    asx_tables = soup.find_all("table")

    # Read in data using pandas
    axs_comp = pd.read_html(str(asx_tables))

    # Quit driver
    driver.quit()

    # Read all data into a dataframe and drop unwanted columns
    comp_df = axs_comp[0].drop(columns=["Rank", "Unnamed: 1", "Unnamed: 4"])

    # Extract numbers from string in market capitalisation column
    comp_df["MARKET_CAP"] = comp_df["Mkt Cap"].str.extract("(\d+)")

    # Convert market cap figure to an integer value
    comp_df["MARKET_CAP"] = comp_df["MARKET_CAP"].astype(float)

    # Extract letters from string
    comp_df["SIZE"] = comp_df["Mkt Cap"].apply(
        lambda x: " ".join(re.split("[^a-zA-Z]*", x))
    )

    # Strip all whitespace from column
    comp_df["SIZE"] = comp_df["SIZE"].str.replace(" ", "")

    # Adjust market capitilisation based off company size (billions)
    comp_df["MARKET_CAP"] = np.where(
        comp_df["SIZE"] == "B", comp_df["MARKET_CAP"] * 1000000000, comp_df["MARKET_CAP"]
    )

    # Adjust market capitilisation based off company size (millions)
    comp_df["MARKET_CAP"] = np.where(
        comp_df["SIZE"] == "M", comp_df["MARKET_CAP"] * 1000000, comp_df["MARKET_CAP"]
    )

    # Adjust market capitilisation based off company size (thousands)
    comp_df["MARKET_CAP"] = np.where(
        comp_df["SIZE"] == "TH", comp_df["MARKET_CAP"] * 1000, comp_df["MARKET_CAP"]
    )

    # Filter out companies without a sector
    comp_df = comp_df[comp_df["Sector"] != "-"]

    # Drop unwanted columns
    comp_df.drop(columns=["Mkt Cap", "SIZE"], inplace=True)

    # Obtain ticker list
    ticker_list = list(comp_df["Code"])
    
    # Return ticker list
    return ticker_list