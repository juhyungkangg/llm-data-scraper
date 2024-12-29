import os
import json
import time
import random
import logging
from datetime import datetime
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
from webdriver_manager.chrome import ChromeDriverManager

# ========================== Configuration ========================== #

URL_DIR = 'nasdaq_data'
FILENAME = 'urls_2024-10-05.jsonl'
OUTPUT_FILENAME = f'articles_{datetime.now().strftime("%Y-%m-%d")}.jsonl'

HEADLESS = True  # Set to True to run in headless mode

# List of realistic User-Agent strings for rotation
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
    'AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/115.0.0.0 Safari/537.36',

    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
    'AppleWebKit/605.1.15 (KHTML, like Gecko) '
    'Version/15.1 Safari/605.1.15',

    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:114.0) '
    'Gecko/20100101 Firefox/114.0',

    'Mozilla/5.0 (X11; Linux x86_64) '
    'AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/115.0.0.0 Safari/537.36',

    # Add more User-Agent strings as needed
]

# ========================== Logging Configuration ========================== #

logging.basicConfig(
    filename='article_scraper.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

# ========================== Helper Functions ========================== #

def save_to_jsonl(article_data, directory, filename):
    """Append a single article's data to a JSONL file immediately."""
    os.makedirs(directory, exist_ok=True)
    filepath = os.path.join(directory, filename)
    try:
        with open(filepath, 'a', encoding='utf-8') as f:
            json_line = json.dumps(article_data, ensure_ascii=False)
            f.write(f"{json_line}\n")
        logging.info(f"Appended article to {filepath}")
    except Exception as e:
        logging.error(f"Failed to append data to {filepath}: {e}")

def load_urls(directory, filename):
    """Load URLs from a JSONL file."""
    urls = []
    prev_urls = []
    filepath = os.path.join(directory, filename)
    if not os.path.exists(filepath):
        logging.error(f"File not found: {filepath}")
        return urls

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    data = json.loads(line)
                    if 'url' in data:
                        urls.append(data['url'])
                except json.JSONDecodeError:
                    logging.warning(f"Invalid JSON line skipped in {filename}")
        with open(os.path.join(directory, 'articles_2024-10-05.jsonl'), 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    data = json.loads(line)
                    if 'url' in data:
                        prev_urls.append(data['url'])
                except json.JSONDecodeError:
                    logging.warning(f"Invalid JSON line skipped in {filename}")
    except Exception as e:
        logging.error(f"Error reading file {filepath}: {e}")

    urls = [url for url in urls if url not in prev_urls]

    logging.info(f"Loaded {len(urls)} URLs from {filepath}")
    return urls

def setup_driver(headless=True):
    """Initialize and return a Selenium WebDriver with realistic settings."""
    options = Options()
    user_agent = random.choice(USER_AGENTS)
    options.add_argument(f'user-agent={user_agent}')

    if headless:
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')

    options.add_argument('--no-sandbox')
    options.add_argument("--window-size=1920,1080")
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)

    service = Service(ChromeDriverManager().install())
    try:
        driver = webdriver.Chrome(service=service, options=options)
    except WebDriverException as e:
        logging.error(f"Error initializing ChromeDriver: {e}")
        raise e

    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": """
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            })
        """
    })

    logging.info("WebDriver initialized successfully.")
    return driver

def human_like_delay(a=2, b=5):
    """Introduce a random delay to mimic human behavior."""
    delay = random.uniform(a, b)
    logging.info(f"Sleeping for {delay:.2f} seconds to mimic human behavior.")
    time.sleep(delay)

def fetch_article_data(driver, url):
    """Fetch the title, date, and body of an article given its URL."""
    article_data = {}
    try:
        driver.get(url)
        logging.info(f"Navigated to URL: {url}")
    except Exception as e:
        logging.error(f"Failed to load URL {url}: {e}")
        return None

    human_like_delay(2, 4)  # Wait for the page to load

    # Extract title
    try:
        title_element = None
        try:
            title_element = driver.find_element(By.CSS_SELECTOR, '.jupiter22-c-hero-article__ > h1')
        except:
            try:
                title_element = driver.find_element(By.CSS_SELECTOR, 'h1 > span')
            except:
                pass
        if not title_element is None:
            title = title_element.text.strip()
            article_data['title'] = title
            logging.info(f"Extracted title: {title}")
        else:
            title = title_element
            logging.info(f"Extracted title: {title}")
    except (NoSuchElementException, TimeoutException):
        logging.warning(f"Title not found for URL: {url}")
        article_data['title'] = None

    # Extract date
    try:
        date_element = None
        try:
            date_element = driver.find_element(By.CSS_SELECTOR, 'div.jupiter22-c-author-byline > p.jupiter22-c-author-byline__timestamp')
        except:
            try:
                date_element = driver.find_element(By.CSS_SELECTOR, 'div.article-header__metadata > div.timestamp > time')
            except:
                pass
        if not date_element is None:
            date_text = date_element.text.strip()
            article_data['date'] = date_text
            logging.info(f"Extracted date: {date_text}")
        else:
            date_text = date_element
            article_data['date'] = date_text
    except NoSuchElementException:
        logging.warning(f"Date not found for URL: {url}")
        article_data['date'] = None

    # Extract body
    try:
        body_element = driver.find_element(By.CSS_SELECTOR, '.body__content')
        body_text = body_element.text.strip()
        article_data['body'] = body_text
        logging.info(f"Extracted body for URL: {url}")
    except NoSuchElementException:
        logging.warning(f"Body content not found for URL: {url}")
        article_data['body'] = None

    article_data['url'] = url  # Include the URL in the data
    return article_data

# ========================== Main Scraping Logic ========================== #

def main():
    # Load URLs from the specified JSONL file
    urls = load_urls(URL_DIR, FILENAME)
    if not urls:
        logging.error("No URLs to process. Exiting script.")
        return

    # Initialize Selenium WebDriver
    try:
        driver = setup_driver(headless=HEADLESS)
    except WebDriverException:
        logging.critical("Failed to initialize WebDriver. Exiting script.")
        return

    try:
        for idx, url in enumerate(urls, start=1):
            logging.info(f"Processing URL {idx}/{len(urls)}: {url} {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            article = fetch_article_data(driver, url)
            if article:
                # Append the article data to the JSONL file immediately
                save_to_jsonl(article, URL_DIR, OUTPUT_FILENAME)
            else:
                logging.warning(f"Skipping URL due to fetch failure: {url}")

            # Introduce a short delay between processing URLs
            human_like_delay(1, 3)

    except Exception as e:
        logging.critical(f"An unexpected error occurred during scraping: {e}")
    finally:
        driver.quit()
        logging.info("WebDriver has been closed.")

if __name__ == "__main__":
    main()
