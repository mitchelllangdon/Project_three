from nltk.tokenize import word_tokenize, sent_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer, PorterStemmer
import nltk
from string import punctuation
import re
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
import psycopg2
import os
from selenium import webdriver
import selenium
import bs4 as bs
from bs4 import BeautifulSoup

# Load environment variables
load_dotenv()

# Creds for PostgreSQL connection
sql_username=os.getenv("sql_username")
sql_pwd=os.getenv("sql_pwd")

# Create a connection to the database
engine = create_engine(f"postgresql://{sql_username}:{sql_pwd}@localhost:5432/hot_copper_db")

# Instantiate the lemmatizer
lemmatizer = WordNetLemmatizer()

# Create a list of stopwords
stopwords = list(stopwords.words("english"))

# Additional stopwords
additional_stopwords = ["said", "also"]

# Expand the default stopwords list if necessary
for stop in additional_stopwords:

    # Append list
    stopwords.append(stop)

# Complete the tokenizer function
def tokenizer(text):
    """Tokenizes text."""

    # Remove the punctuation from text
    regex = re.compile("[^a-zA-Z ]")
    re_clean = regex.sub("", str(text))

    # Create a tokenized list of the words
    words = word_tokenize(re_clean)

    # Lemmatize words into root words
    lem = [lemmatizer.lemmatize(word) for word in words]

    # Remove the stop words
    tokens = [word.lower() for word in words if word.lower() not in stopwords]

    # Return output
    return tokens

# Create an empty list for top comments
top_comments = []

# Read in  hc_stock_sum table from the DB
query = """
SELECT *
FROM hc_stock_sum
         """

# Create a DataFrame from the query result
hc_stock_sum = pd.read_sql(query, engine)

# Create an empty list for top comments
top_comments = []
hc_stock_sum_links = hc_stock_sum[hc_stock_sum["Likes"] >= 20]["HREF_Link"].tolist()

# Open driver
driver = webdriver.Chrome()

# Initiate for loop
for item in hc_stock_sum[hc_stock_sum["Likes"] >= 20]["HREF_Link"]:
    
    # Open new driver
    driver.get(item)

    # Set page source to variable for Beautiful Soup to analyse
    page_info = driver.page_source

    # Initiate beautiful soup instance
    soup = BeautifulSoup(page_info)

    # Find all articles and use list comprehension to store in variable
    info = [e.get_text() for e in soup.find_all("article")]

    # Append list
    top_comments.append(info)

# Combine information into dataframe containing links and comments
combined_df = pd.DataFrame({"HREF": hc_stock_sum_links, "Text": top_comments})

# Extract text from list item in text column
combined_df["Text"] = combined_df["Text"].str[0]

# Drop Nulls
combined_df.dropna()

# Cast Text as string dtype
combined_df["Text"] = combined_df["Text"].astype(str)

# Remove punctuation
combined_df["Text"] = combined_df["Text"].apply(lambda x: x.replace("\n", ""))

# Apply tokenized function to text
combined_df["Tokenize"] = combined_df["Text"].apply(lambda x: tokenizer(x))

# Split data if unnecessary information is present in comment
combined_df["Text"] = combined_df["Text"].apply(
    lambda x: x.split("↑")[1] if "↑" in x else x
)

# Write compiled summary data to DB
combined_df.to_sql('hc_top_likes', con=engine, if_exists='replace')