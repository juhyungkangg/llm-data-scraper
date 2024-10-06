import requests
import json
import time
import os

# ----------------------------
# Configuration and Parameters
# ----------------------------

# Set data directory
DATA_DIRECTORY = 'benzinga_data'

# Ensure the DATA_DIRECTORY exists
os.makedirs(DATA_DIRECTORY, exist_ok=True)

# Replace with your actual Benzinga API key
API_KEY = "70b17a7c90d149ef98388fbe981771e5"  # **Note:** Keep your API key secure!

# Base URL for the Benzinga News API
BASE_URL = "https://api.benzinga.com/api/v2/news"

# Date for which to retrieve news
TARGET_DATE = "2024-09-30"

# Output file to save the fetched news articles
OUTPUT_FILE = f"benzinga_news_{TARGET_DATE}.jsonl"

# Pagination settings
PAGE_SIZE = 50  # Number of articles per page (adjust based on API limits)
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

    while current_page <= max_pages:
        print(f"Fetching page {current_page}...")

        # Define query parameters for the API request
        query_params = {
            "token": api_key,
            "date": date,
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

    return all_articles


def save_to_jsonl(data, filename):
    """
    Saves the given data to a JSON Lines (jsonl) file.

    Args:
        data (list): The data to save.
        filename (str): The filename for the jsonl file.
    """
    filepath = os.path.join(DATA_DIRECTORY, filename)
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            for article in data:
                json_line = json.dumps(article, ensure_ascii=False)
                f.write(json_line + "\n")
        print(f"Successfully saved {len(data)} articles to {filepath}.")
    except IOError as io_err:
        print(f"IO error occurred while saving to file: {io_err}")
    except Exception as e:
        print(f"An unexpected error occurred while saving to file: {e}")


# ----------------------------
# Main Execution Flow
# ----------------------------

def main():
    print(f"Starting to fetch Benzinga news articles for {TARGET_DATE}...")

    # Fetch all news articles
    news_articles = fetch_news(
        api_key=API_KEY,
        date=TARGET_DATE,
        page_size=PAGE_SIZE,
        max_pages=MAX_PAGES
    )

    print(f"Total articles fetched: {len(news_articles)}")

    if news_articles:
        # Save the fetched articles to a JSON Lines file
        save_to_jsonl(news_articles, OUTPUT_FILE)
    else:
        print("No articles were fetched. Please check your API key and parameters.")


if __name__ == "__main__":
    main()
