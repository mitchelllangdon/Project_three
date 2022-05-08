import streamlit as st
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
import psycopg2
import os
import time
import seaborn as sns
import matplotlib.pyplot as plt
import plotly.figure_factory as ff
import plotly

plt.style.use('seaborn')

# Load environment variables
load_dotenv()

# Creds for PostgreSQL connection
sql_username=os.getenv("sql_username")
sql_pwd=os.getenv("sql_pwd")

# Create a connection to the database
engine = create_engine(f"postgresql://{sql_username}:{sql_pwd}@localhost:5432/hot_copper_db")

# Read in  hc_stock_sum table from the DB
hc_stock_sum_query = """
SELECT *
FROM hc_stock_sum
         """

# Read in  hc_top_likes table from the DB
hc_ticker_list_query = """
SELECT *
FROM hc_ticker_list
         """

# Read in  hc_top_likes table from the DB
top_likes_query = """
SELECT *
FROM hc_top_likes
         """

# Create a DataFrames from the query result
hc_stock_sum = pd.read_sql(hc_stock_sum_query, engine)
hc_ticker_list = pd.read_sql(hc_ticker_list_query, engine)
hc_top_likes = pd.read_sql(top_likes_query, engine)

# Charts (ADAM: postgres queries not working with direct column selects - e.g. select href from table)
top_tickers = hc_stock_sum[['Ticker','Likes']].groupby(
    'Ticker').sum().sort_values(
    by = 'Likes', ascending = False).head(10)


# SQL Query for tickers 
comments_trend = """
select
    count("HREF_Link") num_comments, 
    "Ticker"
from 
    hc_stock_sum
GROUP BY
    "Ticker", "Date"
ORDER BY 
    count("HREF_Link") DESC
FETCH FIRST 10 ROWS ONLY"""

# Read into df
comments_df = pd.read_sql(comments_trend, engine)
    
# Only get subset of hc_stock_sum as it is a large database
hc_stock_sum_ordered = hc_stock_sum.sort_values(by=['Likes'], ascending=False).head(10000)

# Drop unwanted columns
hc_stock_sum_ordered.drop(columns = 'Ticker_Filter', inplace = True)
########################################################################
# Streamlit Code

# Set the page configuration, titles and addition of additional markdown
st.set_page_config(page_title = "Raven Analytics", layout = "wide")
st.title("Raven Analytics")
st.markdown("We use the latest technology to create free tools for you to understand all the latest information being said about your ASX stocks.")

# Split into two separate columns for Steamlit page layout
col1, col2 = st.columns(2)

# Initial column
with col1:

# Display all data from the database
    
    # Subheader
    st.subheader("Visualisation")
    
    # Markdown for header
    st.markdown("The following chart shows the most liked stocks based on comments made on stock forums.")
    
    # First chart for top 10 tickers with most likes
    top_likes_fig = st.bar_chart(data = top_tickers, use_container_width = True)
    
    # Print Streamlit Tables from Database
    st.markdown("The following shows the most liked comments on Hotcopper based on recent data collection.")
    st.write(hc_stock_sum_ordered)
    
    # Print Streamlit Tables from Database
    st.markdown("# Hotcopper Tickers")
    st.write(hc_ticker_list)

    # Print Streamlit Tables from Database
    st.markdown("# Hotcopper Top Likes")
    st.write(hc_top_likes)
    
    # Subheader for most talked about
    st.subheader('Most active/talked about stocks')
    st.markdown('The following captures the stocks that are most talked about.')
    
    # Most talked about figure
    top_comments_fig = st.bar_chart(data = comments_df.set_index('Ticker'), use_container_width = True)
    
# Second column for selfe serve
with col2:
    
    # Subheading for SQL queries
    st.subheader("Self-Service Analytics:")
    
    # Markdown below subheader
    st.markdown("For users experienced in SQL (or who even want to learn), try querying our live database for information about your stock. Refer to the tables and data dictionaries below for further guidance.")
    
    # Tables
    st.markdown("""
    There are three tables used to store our data. These include :
     * **HC_STOCK_SUM**: Includes comment volume and likes about your stock including the poster and the HREF. Note that HREF is the key to joining to the comments table (sample query below)
     * **HC_TICKER_LIST**: Stuck on a ticker to search for? Refer to our database to find tickers for ASX companies. 
     * **HC_TOP_LIKES**: This table includes the comments that received the most likes from users on HotCopper. Use this table to join back to HC_STOCK_SUM
     
    Sample queries include:
     * `SELECT * FROM HC_STOCK_SUM`
     * `SELECT * FROM HC_TOP_LIKES`
     * `SELECT "HREF" FROM HC_TOP_LIKES`
     * `SELECT a."Ticker", a."Poster", a."Likes", b."Text" from hc_stock_sum a inner join hc_top_likes b on a."HREF_Link" = b."HREF" ORDER BY 3 DESC`
     
    Note to user: when querying individual columns, ensure to place double quotations around the column name as per the last example above.
     """)
    
    # Create text input that is stored in variable for later use
    self_serve_query = st.text_input("For SQL users, input your own query here (table summary above):", max_chars = None)
    
    # Error handling for incorect queries
    try:
        
        # Take in query from text input
        user_query = self_serve_query
        
        # Create dataframe with the sql query
        output_user_query = pd.read_sql(user_query, engine)
        
        # Upon successful completion of the for loop, print successful query
        st.success("Query is valid. Please wait for your data.")
        
        # Additional features for dashboard functionality
        progress_bar = st.progress(0)
        
        # For loop for progress bar and printing successful query to user
        for percent_complete in range(100):
            
            # Time module for sleep
            time.sleep(0.1)
            
            # Add to progress bar
            progress_bar.progress(percent_complete + 1)
        
        # Output of results
        st.dataframe(output_user_query)
        
        
    # Raise exception if query did not run properly (generic error message)
    except:
        st.warning("Please try entering a valid query")
    
    # Next section for sentiment analysis
    st.subheader("ASX Stock Sentiment: What are investors saying about your stocks?")
    
    # Markdown
    ticker_input = st.text_input("Please enter a valid 3 letter ticker:")
    
    # Obtain text
    nlp_query = f"""
    SELECT
        b."Text"
    from
        hc_stock_sum a inner join hc_top_likes b on a."HREF_Link" = b."HREF"
    WHERE a."Ticker" = '{ticker_input}'
    """
    
    # Read into dataframe
    nlp_data = pd.read_sql(nlp_query, engine)
    
    try:
        
        # Display sample results
        st.dataframe(nlp_data)
        
        #### ADAM: WORDCLOUD here with text from nlp_data above
    except:
        
        st.warning("Please enter a valid ticker")