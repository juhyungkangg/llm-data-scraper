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

NASDAQ_DATA_DIR = '../../data/nasdaq_data/articles'
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
    filename='nasdaq_scraper.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

# ========================== Helper Functions ========================== #

def save_to_jsonl(articles, directory):
    """Append a single article's data to a JSONL file immediately."""
    current_date = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    filepath = os.path.join(directory, f'nasdaq_articles_{current_date}.jsonl')
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            for article in articles:
                json_line = json.dumps(article)
                f.write(f"{json_line}\n")
        logging.info(f"Appended article to {filepath}")
    except Exception as e:
        logging.error(f"Failed to append data to {filepath}: {e}")

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
        try:
            title_element = driver.find_element(By.CSS_SELECTOR, '.jupiter22-c-hero-article__ > h1')
            title = title_element.text.strip()
            article_data['title'] = title
            logging.info(f"Extracted title: {title}")
        except:
            try:
                title_element = driver.find_element(By.CSS_SELECTOR, 'h1 > span')
                title = title_element.text.strip()
                article_data['title'] = title
                logging.info(f"Extracted title: {title}")
            except:
                pass
    except (NoSuchElementException, TimeoutException):
        logging.warning(f"Title not found for URL: {url}")
        article_data['title'] = None

    # Extract date
    try:
        try:
            date_element = driver.find_element(By.CSS_SELECTOR, 'div.jupiter22-c-author-byline > p.jupiter22-c-author-byline__timestamp')
            date_text = date_element.text.strip()
            article_data['date'] = date_text
            logging.info(f"Extracted date: {date_text}")
        except:
            try:
                date_element = driver.find_element(By.CSS_SELECTOR, 'div.article-header__metadata > div.timestamp > time')
                date_text = date_element.text.strip()
                article_data['date'] = date_text
                logging.info(f"Extracted date: {date_text}")
            except:
                pass
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

def scrape_nasdaq_articles(urls):
    # Initialize Selenium WebDriver
    try:
        driver = setup_driver(headless=HEADLESS)
    except WebDriverException:
        logging.critical("Failed to initialize WebDriver. Exiting script.")
        return

    try:
        articles = []
        for idx, url in enumerate(urls, start=1):
            logging.info(f"Processing URL {idx}/{len(urls)}: {url}")
            print(f"Processing URL {idx}/{len(urls)}: {url}")
            article = fetch_article_data(driver, url)
            articles.append(article)

            # Introduce a short delay between processing URLs
            human_like_delay(3, 8)

        save_to_jsonl(articles, NASDAQ_DATA_DIR)

        return articles
    except Exception as e:
        logging.critical(f"An unexpected error occurred during scraping: {e}")
    finally:
        driver.quit()
        logging.info("WebDriver has been closed.")

