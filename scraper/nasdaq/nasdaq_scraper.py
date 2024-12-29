import os
import json
import time
import random
import logging
import hashlib
from datetime import datetime
import pytz
import schedule

# MySQL Connector and Error Handling
import mysql.connector
from mysql.connector import Error

# Environment variable management
from dotenv import load_dotenv

# Web scraping with Selenium
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import (
    NoSuchElementException,
    ElementClickInterceptedException,
    TimeoutException,
    WebDriverException,
    StaleElementReferenceException
)
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service

# ChromeDriver management
from webdriver_manager.chrome import ChromeDriverManager

# BeautifulSoup for HTML parsing
from bs4 import BeautifulSoup

# Custom modules for Nasdaq scraping
from nasdaq_url_getter_for_scraping import *
from nasdaq_news_getter_for_scraping import *


table_name = 'nasdaq_db'


load_dotenv()

# Configure logging
logging.basicConfig(
    filename='nasdaq_scraper.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)


# Database connection
def create_connection():
    try:
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_DATABASE')
        )
        if connection.is_connected():
            print("Connected to MySQL database")
            return connection
    except Error as e:
        print(f"Error: {e}")
        return None

# Function to check if data exists in the table
def get_urls(connection):
    check_query = f"SELECT url FROM {table_name};"  # Modify the query as needed
    cursor = connection.cursor(dictionary=True)  # dictionary=True to return rows as dicts

    try:
        cursor.execute(check_query)
        results = cursor.fetchall()

        if results:
            print(f"Data found in '{table_name}':")
            urls = [x['url'] for x in results]
            return urls
        else:
            print(f"No data found in '{table_name}'.")
            return None
    except Error as e:
        print(f"Error while checking data: {e}")

# Function to convert date string to EDT datetime
def convert_to_edt_datetime(date_str):
    # Define possible date formats
    formats = [
        '%B %d, %Y — %I:%M %p %Z',  # "October 04, 2024 — 10:50 am EDT"
        '%b %d, %Y %I:%M%p %Z',  # "DEC 20, 2022 10:20AM EST"
        '%B %d, %Y — %I:%M %p',  # Without timezone abbreviation
        '%b %d, %Y %I:%M%p',  # "DEC 8, 2021 9:31AM"
        '%b %d, %Y %I:%M%p %Z',  # "AUG 16, 2023 1:13PM EDT"
        '%B %d, %Y — %I:%M %p'  # "October 04, 2024 — 10:50 am" (no timezone)
    ]

    # Try parsing the date string using the possible formats
    for fmt in formats:
        try:
            # Remove the timezone abbreviation from the string for processing
            clean_date_str = date_str.replace(' EDT', '').replace(' EST', '')
            naive_datetime = datetime.strptime(clean_date_str, fmt)

            # Assign the correct timezone (Eastern Time)
            eastern = pytz.timezone('US/Eastern')

            # Determine if it's EDT or EST based on the input string
            if 'EDT' in date_str:
                localized_datetime = eastern.localize(naive_datetime, is_dst=True)  # Daylight Saving Time
            elif 'EST' in date_str:
                localized_datetime = eastern.localize(naive_datetime, is_dst=False)  # Standard Time
            else:
                localized_datetime = eastern.localize(naive_datetime)

            # Return the datetime in SQL format
            return localized_datetime.strftime('%Y-%m-%d %H:%M:%S')

        except ValueError:
            continue  # Try the next format if the current one doesn't match

    print(f"Date format not recognized: {date_str}")
    return None


def generate_id_from_url(url):
    # Use SHA-256 hash to create a unique ID from the URL
    url_hash = hashlib.sha256(url.encode('utf-8')).hexdigest()

    # Optionally, take only a subset of the hash to shorten the ID (e.g., first 16 characters)
    unique_id = url_hash[:16]  # You can adjust the length if needed

    return unique_id


# Insert Nasdaq data into the table
def insert_data(connection, data):
    insert_query = """
    INSERT INTO nasdaq_db (id, title, datetime, body, url, source_id)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        title=VALUES(title), datetime=VALUES(datetime), body=VALUES(body),
        url=VALUES(url), source_id=VALUES(source_id);
    """
    cursor = connection.cursor()

    # Insert the data with source_id = 2 for Nasdaq
    for row in data:
        if row is None:
            continue
        cursor.execute(insert_query, (
            generate_id_from_url(row.get('url','')),
            row.get('title'),
            convert_to_edt_datetime(row.get('date','')),
            row.get('body'),
            row.get('url'),
            2  # source_id for Nasdaq is 2
        ))
    connection.commit()


# Main function to run the process
def main():
    # Get URL from db
    connection = create_connection()

    urls = []
    if connection:
        urls = get_urls(connection)
        connection.close()
        print("MySQL connection closed.")

    # Get URL from nasdaq
    scraped_urls = scrape_nasdaq_urls()
    new_urls = [url for url in scraped_urls if url not in urls]

    # Scrape new urls
    articles = scrape_nasdaq_articles(new_urls)

    # Upload data to db
    connection = create_connection()
    if connection:
        insert_data(connection, articles)
        print(f"Updated {len(articles)} articles to Nasdaq database.{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logging.info(f"Updated {len(articles)} articles to Nasdaq database. {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        connection.close()
        print("MySQL connection closed.")

# Schedule the function to run at specific times
for hour in range(0, 24, 2):
    time_str = f"{hour:02d}:40"
    schedule.every().day.at(time_str).do(main)


if __name__ == '__main__':
    main()
    while True:
        schedule.run_pending()
        time.sleep(1)




