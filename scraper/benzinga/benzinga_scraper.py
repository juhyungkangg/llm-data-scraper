import requests
import json
import time
import os
from mysql.connector import Error
from datetime import datetime, timedelta
import pytz
from dotenv import load_dotenv
import os
import json
import schedule
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from datetime import datetime
import pytz
import pandas as pd

load_dotenv()

# ----------------------------
# Configuration and Parameters
# ----------------------------

# Replace with your actual Benzinga API key
API_KEY = os.getenv('BENZINGA_API_KEY')

# Base URL for the Benzinga News API
BASE_URL = "https://api.benzinga.com/api/v2/news"

# Pagination settings
PAGE_SIZE = 100  # Number of articles per page (adjust based on API limits)
MAX_PAGES = 100  # Maximum number of pages to fetch to prevent infinite loops

# Headers for the HTTP request
HEADERS = {
    "Accept": "application/json"
}

# ----------------------------
# Function Definitions
# ----------------------------

def fetch_news(api_key, date, page_size=50, max_pages=100):
    """
    Fetches all news articles from Benzinga for a specific date.

    Args:
        api_key (str): Your Benzinga API key.
        date (str): The date for which to retrieve news (format: YYYY-MM-DD).
        page_size (int, optional): Number of articles per page. Defaults to 50.
        max_pages (int, optional): Maximum number of pages to fetch. Defaults to 100.

    Returns:
        list: A list of all fetched news articles.
    """
    all_articles = []
    current_page = 1
    max_date = pd.to_datetime('1900-01-01')

    while current_page <= max_pages:
        print(f"Fetching page {current_page}...")

        # Define query parameters for the API request
        query_params = {
            "token": api_key,
            "dateFrom": date,
            "displayOutput": "full",  # Options: 'abstract', 'full', etc.
            "page": current_page,
            "pageSize": page_size
        }

        try:
            response = requests.get(BASE_URL, headers=HEADERS, params=query_params)
            response.raise_for_status()  # Raises HTTPError for bad responses (4xx or 5xx)

            data = response.json()

            # Inspect the response structure
            if isinstance(data, dict):
                # Assuming articles are under the 'articles' key
                articles = data.get("articles", [])

                # If 'articles' key doesn't exist, assume the entire response is a list
                if not articles:
                    articles = data if isinstance(data, list) else []
            elif isinstance(data, list):
                # If the response is a list, assign it directly
                articles = data
            else:
                # Unexpected format
                print("Unexpected response format. Unable to locate articles.")
                break

            if not articles:
                print("No more articles found.")
                break  # Exit the loop if no articles are returned

            # Update last date
            for article in articles:
                article_date = article.get('created', '1900-01-01')
                article_date = pd.to_datetime(convert_to_edt_datetime(article_date))
                if article_date > max_date:
                    max_date = article_date

            all_articles.extend(articles)
            print(f"Fetched {len(articles)} articles from page {current_page}.")

            # Optional: Respect API rate limits by adding a delay
            time.sleep(1)  # Sleep for 1 second between requests

            current_page += 1  # Move to the next page

        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
            print("Terminating the fetch process.")
            break
        except requests.exceptions.RequestException as req_err:
            print(f"Request exception: {req_err}")
            print("Terminating the fetch process.")
            break
        except json.JSONDecodeError as json_err:
            print(f"JSON decode error: {json_err}")
            print("Terminating the fetch process.")
            break
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            print("Terminating the fetch process.")
            break

    return all_articles, max_date

# ----------------------------
# SQL Setting
# ----------------------------

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

def extract_text_from_html(html):
    # Create a BeautifulSoup object to parse the HTML
    soup = BeautifulSoup(html, 'html.parser')

    # Extract the text from the soup object
    text = soup.get_text(separator=" ")

    # Clean up the extra spaces
    cleaned_text = ' '.join(text.split())

    return cleaned_text

def convert_to_edt_datetime(date_str):
    # Parse the date string into a naive datetime object (ignoring timezone for now)
    naive_datetime = datetime.strptime(date_str, '%a, %d %b %Y %H:%M:%S %z')

    # Convert to timezone-aware datetime in EDT
    edt_timezone = pytz.timezone('US/Eastern')
    edt_datetime = naive_datetime.astimezone(edt_timezone)

    return edt_datetime.strftime('%Y-%m-%d %H:%M:%S')

# Parse jsonl file and insert into table
def insert_data(connection, data):
    insert_query = """
    INSERT INTO benzinga_db (id, author, created, updated, title, teaser, body, url, stocks, channels, source_id)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        author=VALUES(author), created=VALUES(created), updated=VALUES(updated),
        title=VALUES(title), teaser=VALUES(teaser), body=VALUES(body), url=VALUES(url),
        stocks=VALUES(stocks), channels=VALUES(channels), source_id=VALUES(source_id);
    """
    cursor = connection.cursor()

    for row in data:
        cursor.execute(insert_query, (
            row.get('id'),
            row.get('author'),
            convert_to_edt_datetime(row.get('created')),
            convert_to_edt_datetime(row.get('updated')),
            row.get('title'),
            extract_text_from_html(row.get('teaser')),
            extract_text_from_html(row.get('body')),
            row.get('url'),
            ','.join([stock['name'] for stock in row.get('stocks', [])]),
            ','.join([channel['name'] for channel in row.get('channels', [])]),
            1  # The source_id for Benzinga data is 1
        ))
    connection.commit()


# ----------------------------
# Main Execution Flow
# ----------------------------

def main(date=None):


    from datetime import datetime, timedelta

    if date == None:
        # Get the current date and time
        current_date = datetime.now()

        # Subtract three days using timedelta
        three_days_before = current_date - timedelta(days=3)
        START_DATE = three_days_before.strftime('%Y-%m-%d')
    else:
        START_DATE = date

    print(f"Starting to fetch Benzinga news articles from {START_DATE}...")

    # Fetch all news articles
    news_articles, max_date = fetch_news(
        api_key=API_KEY,
        date=START_DATE,
        page_size=PAGE_SIZE,
        max_pages=MAX_PAGES
    )

    print(f"Total articles fetched: {len(news_articles)} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    connection = create_connection()
    if connection and news_articles:
        # Save the fetched articles to SQL db
        insert_data(connection, news_articles)
        connection.close()
        print("MySQL connection closed.")

    return max_date

# Schedule the function to run at specific times
for hour in range(1, 24, 2):
    time_str = f"{hour:02d}:00"
    schedule.every().day.at(time_str).do(main)

if __name__ == "__main__":
    main()
    while True:
        schedule.run_pending()
        time.sleep(1)
