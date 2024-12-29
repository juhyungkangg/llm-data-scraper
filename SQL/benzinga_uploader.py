import os
import json
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from datetime import datetime
import pytz

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


# Alter the table to change the 'created' and 'updated' column types to DATETIME
def alter_table_columns_to_datetime(connection):
    try:
        alter_query = """
        ALTER TABLE benzinga_db
        MODIFY created DATETIME,
        MODIFY updated DATETIME;
        """
        cursor = connection.cursor()
        cursor.execute(alter_query)
        connection.commit()
        print("Table columns 'created' and 'updated' successfully altered to DATETIME")
    except Error as e:
        print(f"Error altering table: {e}")

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

# Read all jsonl files from directory
def process_files(connection, directory):
    for filename in os.listdir(directory):
        if filename.endswith('.jsonl'):
            file_path = os.path.join(directory, filename)
            with open(file_path, 'r', encoding='utf-8') as file:
                data = [json.loads(line) for line in file]
                insert_data(connection, data)
            print(f"Data from {filename} inserted successfully.")


# Main function to run the process
def main():
    connection = create_connection()
    if connection:
        # Alter table to update column types before processing data
        alter_table_columns_to_datetime(connection)

        directory = '../data/benzinga_data'  # Specify your directory
        process_files(connection, directory)

        connection.close()
        print("MySQL connection closed.")


if __name__ == '__main__':
    main()
