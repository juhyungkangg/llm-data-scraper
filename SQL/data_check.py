import os
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

table_name = 'seeking_alpha_db'


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

# Function to check if data exists in the table
def check_data(connection):
    check_query = f"SELECT * FROM {table_name} LIMIT 10;"  # Modify the query as needed
    cursor = connection.cursor(dictionary=True)  # dictionary=True to return rows as dicts

    try:
        cursor.execute(check_query)
        results = cursor.fetchall()

        if results:
            print(f"Data found in '{table_name}':")
            for row in results:
                print(row)
        else:
            print(f"No data found in '{table_name}'.")
    except Error as e:
        print(f"Error while checking data: {e}")

# Main function to run the process
def main():
    connection = create_connection()
    if connection:
        check_data(connection)
        connection.close()
        print("MySQL connection closed.")

if __name__ == '__main__':
    main()
