import json
import requests
import os
from bs4 import BeautifulSoup
from datetime import datetime
from dotenv import load_dotenv
import mysql.connector
from mysql.connector import Error

load_dotenv()  # Loads variables from .env

# Parse datetime
def parse_datetime(datetime_str):
    if isinstance(datetime_str, str):
        # Parse the ISO 8601 string to a datetime object
        dt = datetime.fromisoformat(datetime_str)

        # Format it into SQL DATETIME format
        sql_datetime = dt.strftime('%Y-%m-%d %H:%M:%S')

        return sql_datetime


# Extract text from html source
def get_text_from_html(element):
    if isinstance(element, str):
        # Parse the element source using BeautifulSoup
        soup = BeautifulSoup(element, 'html.parser')

        # Extract text content
        text = soup.get_text(separator=" ")

        # Clean up the extra spaces
        cleaned_text = ' '.join(text.split())

        return cleaned_text
    else:
        return element

# ----------------------------
# Load Environment Variables
# ----------------------------
DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_DATABASE = os.getenv('DB_DATABASE')

# ----------------------------
# Database Connection
# ----------------------------

def create_connection():
    """
    Establishes a connection to the MySQL database.

    Returns:
        connection (mysql.connector.connection_cext.CMySQLConnection): MySQL connection object
    """
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_DATABASE
        )
        if connection.is_connected():
            print("Connected to MySQL database")
            return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None


# ----------------------------
# Table Creation
# ----------------------------

def create_table(connection):
    """
    Creates the 'source' and 'seeking_alpha_db' tables if they do not exist.

    Args:
        connection (mysql.connector.connection_cext.CMySQLConnection): MySQL connection object
    """

    create_seeking_alpha_db_table = """
    CREATE TABLE IF NOT EXISTS seeking_alpha_db (
        id VARCHAR(20) PRIMARY KEY,
        title TEXT,
        published_on DATETIME NOT NULL,
        last_modified DATETIME,
        summary TEXT, -- Nullable for news data
        content TEXT NOT NULL,
        url TEXT,
        tickers_primary VARCHAR(255),
        tickers_secondary VARCHAR(1000),
        source_id INT,
        FOREIGN KEY (source_id) REFERENCES sources (source_id)
    );
    """

    cursor = connection.cursor()
    try:
        cursor.execute(create_seeking_alpha_db_table)
        connection.commit()
        print("Table created or already exist.")
    except Error as e:
        print(f"Error creating tables: {e}")
    finally:
        cursor.close()


# ----------------------------
# Insert Source Records
# ----------------------------

def insert_sources(connection):
    """
    Inserts source records into the 'source' table.

    Args:
        connection (mysql.connector.connection_cext.CMySQLConnection): MySQL connection object
    """
    insert_source_query = """
    INSERT INTO sources (source_id, source_name, source_type)
    VALUES (%s, %s, %s)
    ON DUPLICATE KEY UPDATE
        source_name=VALUES(source_name),
        source_type=VALUES(source_type);
    """
    sources = [
        (4, 'seeking_alpha_news', 'seeking_alpha'),
        (5, 'seeking_alpha_article', 'seeking_alpha')
    ]

    cursor = connection.cursor()
    try:
        cursor.executemany(insert_source_query, sources)
        connection.commit()
        print("Source records inserted/updated successfully.")
    except Error as e:
        print(f"Error inserting sources: {e}")
    finally:
        cursor.close()


# ----------------------------
# Insert Seeking Alpha Data
# ----------------------------

def insert_seeking_alpha_data(connection, data, source_id):
    """
    Inserts a single news or article record into the 'seeking_alpha_db' table.

    Args:
        connection (mysql.connector.connection_cext.CMySQLConnection): MySQL connection object
        data (dict): The news or article data as a dictionary.
        source_id (int): The source_id linking to the 'source' table.
    """
    insert_query = """
    INSERT INTO seeking_alpha_db 
    (id, title, published_on, last_modified, summary, content, url, tickers_primary, tickers_secondary, source_id)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        title=VALUES(title),
        published_on=VALUES(published_on),
        last_modified=VALUES(last_modified),
        summary=VALUES(summary),
        content=VALUES(content),
        url=VALUES(url),
        tickers_primary=VALUES(tickers_primary),
        tickers_secondary=VALUES(tickers_secondary),
        source_id=VALUES(source_id);
    """

    # Prepare the data for insertion
    try:
        id_ = data.get('id')
        title = data.get('title')
        published_on = data.get('published_on')
        last_modified = data.get('last_modified')
        summary = data.get('summary', None)  # Can be None for news
        content = data.get('content')
        url = data.get('url', None)
        tickers_primary = ','.join(data.get('tickers_primary', []))
        tickers_secondary = ','.join(data.get('tickers_secondary', []))

        cursor = connection.cursor()
        cursor.execute(insert_query, (
            id_,
            title,
            published_on,
            last_modified,
            summary,
            content,
            url,
            tickers_primary,
            tickers_secondary,
            source_id
        ))
        connection.commit()
    except Error as e:
        print(f"Error inserting record ID {data.get('id')}: {e}")
    except Exception as e:
        print(f"Unexpected error for record ID {data.get('id')}: {e}")
    finally:
        cursor.close()