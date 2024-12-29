import praw
import json
import time
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
import pytz
import dropbox
from dropbox.exceptions import AuthError, ApiError
from apscheduler.schedulers.background import BackgroundScheduler
import requests
from dotenv import load_dotenv
import mysql.connector
from mysql.connector import Error
import schedule


# ----------------------------
# Load Environment Variables
# ----------------------------

load_dotenv()  # Loads variables from .env

REDDIT_CLIENT_ID = os.getenv('REDDIT_CLIENT_ID')
REDDIT_CLIENT_SECRET = os.getenv('REDDIT_CLIENT_SECRET')
REDDIT_USER_AGENT = os.getenv('REDDIT_USER_AGENT')

DROPBOX_ACCESS_TOKEN = os.getenv('DROPBOX_ACCESS_TOKEN')

TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

# ----------------------------
# Configuration and Parameters
# ----------------------------

# List of subreddits to monitor
SUBREDDITS = [
    'stocks',
    'investing',
    'wallstreetbets',
    'options',
    'daytrading',
    'StockMarket',
    'pennystocks',
    'SwingTrading',
    'TechnicalAnalysis',
    'QuantitativeFinance',
    'algotrading',
    'RobinHood',
    'Finance',
    'DividendInvesting',
    'HighFrequencyTrading',
    'Economics',
    'EToro',
    'Forex',
    'Bogleheads'
]

# Output directory (ensure this directory exists or will be created)
OUTPUT_DIR = '../../data/reddit_data'


# Logging configuration
LOG_FILE = 'reddit_scraper.log'
os.makedirs(OUTPUT_DIR, exist_ok=True)
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


# ----------------------------
# Telegram Notification Function
# ----------------------------

def send_telegram_message(message):
    """
    Sends a message to the specified Telegram chat using the bot.
    """
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'text': message,
        'parse_mode': 'Markdown'
    }
    try:
        response = requests.post(url, data=payload)
        if response.status_code == 200:
            logging.info("Telegram message sent successfully.")
        else:
            logging.error(
                f"Failed to send Telegram message. Status Code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        logging.error(f"Exception occurred while sending Telegram message: {e}")




# ----------------------------
# Initialize Reddit Instance
# ----------------------------

def initialize_reddit(client_id, client_secret, user_agent):
    """
    Initializes and returns a Reddit instance using PRAW.
    Sends Telegram message upon authentication failure.
    """
    try:
        reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent
        )
        # Test the connection
        reddit.user.me()
        logging.info("Successfully authenticated with Reddit.")
        # send_telegram_message("*Info:* Successfully authenticated with Reddit.")
        return reddit
    except Exception as e:
        logging.error(f"Failed to initialize Reddit instance: {e}")
        send_telegram_message(f"*Error:* Failed to authenticate with Reddit.\n`{e}`")
        raise



# ----------------------------
# Process and Save Submission
# ----------------------------

def process_submission(submission):
    """
    Processes a single submission. If it's new, appends it to the file and updates existing_ids.
    Sends Telegram notifications upon saving new submissions.
    """

    submission_data = {
        'id': submission.id,
        'subreddit': str(submission.subreddit),
        'created_utc': datetime.utcfromtimestamp(submission.created_utc).isoformat() + 'Z',
        'title': submission.title,
        'selftext': submission.selftext,
        'url': submission.url,
        'score': submission.score,
        'num_comments': submission.num_comments,
        'ups': submission.ups,
        'author': str(submission.author) if submission.author else 'N/A',
        # 'permalink': submission.permalink
    }

    # logging.info(f"New submission saved: {submission.id} - {datetime.utcfromtimestamp(submission.created_utc).isoformat() + 'Z'} - {str(submission.subreddit)}")
    # print(submission_data)

    return submission_data


# ----------------------------
# Fetch Historical Submissions
# ----------------------------

def fetch_historical_submissions(reddit, subreddits, limit=1000):
    """
    Fetches historical submissions from the list of subreddits and saves new ones.
    """
    try:
        # Combine subreddit names into a single string separated by '+'
        subreddit_str = '+'.join(subreddits)
        subreddit = reddit.subreddit(subreddit_str)
        logging.info(f"Fetching up to {limit} historical submissions from r/{subreddit_str}.")
        # send_telegram_message(f"*Info:* Fetching up to {limit} historical submissions from r/{subreddit_str}.")

        submissions = []
        for submission in subreddit.new(limit=limit):
            submissions.append(process_submission(submission))

        logging.info("Finished fetching historical submissions.")
        print(f"Finished fetching historical submissions. Total length: {len(submissions)}")
        # send_telegram_message("*Info:* Finished fetching historical submissions.")

        return submissions

    except Exception as e:
        logging.error(f"An error occurred while fetching historical submissions: {e}")
        # send_telegram_message(f"*Error:* An error occurred while fetching historical submissions.\n`{e}`")

# ----------------------------
# SQL Upload
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

# Convert the 'created_utc' field to Eastern Time DATETIME format for SQL
def convert_to_eastern_datetime(utc_string):
    # Convert from the format '2019-07-01T20:54:49Z' to a Python datetime object
    utc_datetime = datetime.strptime(utc_string, '%Y-%m-%dT%H:%M:%SZ')

    # Convert UTC to Eastern Time
    eastern = pytz.timezone('US/Eastern')
    utc = pytz.utc
    utc_aware = utc.localize(utc_datetime)  # Make UTC time aware
    eastern_datetime = utc_aware.astimezone(eastern)

    # Return as a string in SQL DATETIME format
    return eastern_datetime.strftime('%Y-%m-%d %H:%M:%S')

# Insert reddit post data into the SQL table
def insert_reddit_data(connection, data):
    insert_query = """
    INSERT INTO reddit_submission (id, subreddit, created_utc, title, selftext, url, score, num_comments, ups, author, source_id)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        subreddit=VALUES(subreddit), created_utc=VALUES(created_utc), 
        title=VALUES(title), selftext=VALUES(selftext), url=VALUES(url), 
        score=VALUES(score), num_comments=VALUES(num_comments), ups=VALUES(ups), author=VALUES(author);
    """
    cursor = connection.cursor()

    # Loop through each post and insert it into the database
    for row in data:
        cursor.execute(insert_query, (
            row['id'],
            row['subreddit'],
            convert_to_eastern_datetime(row['created_utc']),
            row['title'],
            row['selftext'],
            row['url'],
            row['score'],
            row['num_comments'],
            row['ups'],
            row['author'],
            3  # Reddit submission is source_id 3
        ))
    connection.commit()


# ----------------------------
# Main Execution Flow
# ----------------------------

def main():
    # Initialize Reddit
    try:
        reddit = initialize_reddit(REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT)
    except Exception as e:
        logging.critical(f"Exiting due to Reddit authentication failure: {e}")
        send_telegram_message("*Critical Error:* Exiting scraper due to Reddit authentication failure.")
        return

    try:
        # Fetch historical submissions
        submissions = fetch_historical_submissions(reddit, SUBREDDITS, limit=3000) # Set limit=None for all available

        connection = create_connection()
        if connection:
            insert_reddit_data(connection, submissions)

            # Close the connection
            connection.close()
            print(f"Updated Reddit database. {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            # logging.info(f"Updated Reddit database. {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            # send_telegram_message("*Info:* Updated Reddit database.")

    except KeyboardInterrupt:
        logging.info("Script interrupted by user. Shutting down.")
        # send_telegram_message("*Info:* Scraper interrupted by user. Shutting down.")
    except Exception as e:
        logging.critical(f"Unexpected error: {e}")
        # send_telegram_message(f"*Critical Error:* An unexpected error occurred.\n`{e}`")


if __name__ == "__main__":
    while True:
        main()
        time.sleep(35 * 60)
