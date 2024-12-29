import os
import json
import mysql.connector
from mysql.connector import Error
from datetime import datetime
import pytz
from dotenv import load_dotenv

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


# Create reddit_submission table
def create_reddit_table(connection):
    try:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS reddit_submission (
            id VARCHAR(15) PRIMARY KEY,
            subreddit VARCHAR(255),
            created_utc DATETIME,
            title TEXT,
            selftext TEXT,
            url TEXT,
            score INT,
            num_comments INT,
            ups INT,
            author VARCHAR(255),
            source_id INT,
            FOREIGN KEY (source_id) REFERENCES sources(source_id)
        );
        """
        cursor = connection.cursor()
        cursor.execute(create_table_query)
        connection.commit()
        print("Table 'reddit_submission' created successfully")
    except Error as e:
        print(f"Error creating table: {e}")

def alter_reddit_table(connection):
    try:
        alter_table_query = """
        ALTER TABLE reddit_submission
        ADD COLUMN url TEXT,
        ADD COLUMN score INT,
        ADD COLUMN num_comments INT,
        ADD COLUMN ups INT,
        ADD COLUMN author VARCHAR(255);
        """
        cursor = connection.cursor()
        cursor.execute(alter_table_query)
        connection.commit()
        print("Table 'reddit_submission' altered successfully")
    except Error as e:
        print(f"Error altering table: {e}")


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


# Process JSONL file and insert into the database
def process_reddit_file(connection, file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        data = [json.loads(line) for line in file]
        insert_reddit_data(connection, data)
    print(f"Data from {file_path} inserted successfully.")


# Main function to create the table and upload the data
def main():
    connection = create_connection()
    if connection:
        # Create the reddit_submission table if it doesn't exist
        create_reddit_table(connection)

        # Alter the table to add missing columns if necessary
        alter_reddit_table(connection)

        # Directory and file path (replace with your actual file path)
        file_path = '../data/reddit_data/reddit_data_1.jsonl'  # Specify your directory

        # Process the file and insert data
        process_reddit_file(connection, file_path)

        # Close the connection
        connection.close()
        print("MySQL connection closed.")


if __name__ == '__main__':
    main()
