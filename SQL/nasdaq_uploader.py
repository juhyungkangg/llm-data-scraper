import os
import json
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from datetime import datetime
import pytz
import hashlib

load_dotenv()


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


# Create nasdaq_db table with source_id as FOREIGN KEY
def create_table(connection):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS nasdaq_db (
        id VARCHAR(16) PRIMARY KEY,
        title TEXT,
        datetime DATETIME,
        body TEXT,
        url TEXT,
        source_id INT,  -- Add source_id column
        FOREIGN KEY (source_id) REFERENCES sources(source_id) -- Define foreign key relationship
    );
    """
    cursor = connection.cursor()
    cursor.execute(create_table_query)
    connection.commit()
    print("Table 'nasdaq_db' created successfully")


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

    raise ValueError(f"Date format not recognized: {date_str}")


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
        cursor.execute(insert_query, (
            generate_id_from_url(row.get('url','')),
            row.get('title'),
            convert_to_edt_datetime(row.get('date','')),
            row.get('body'),
            row.get('url'),
            2  # source_id for Nasdaq is 2
        ))
    connection.commit()


# Process a single JSON file and insert into the database
def process_file(connection, file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        data = [json.loads(line) for line in file]
        insert_data(connection, data)
    print(f"Data from {file_path} inserted successfully.")


# Main function to run the process
def main():
    connection = create_connection()
    if connection:
        create_table(connection)
        file_path = '../data/nasdaq_data/articles/nasdaq_articles_2024-10-06-20-40-31.jsonl'  # Specify your file path
        process_file(connection, file_path)
        connection.close()
        print("MySQL connection closed.")


if __name__ == '__main__':
    main()
