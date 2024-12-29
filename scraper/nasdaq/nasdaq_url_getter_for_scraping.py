import os
import json
import time
import random
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
import logging

# ========================== Configuration ========================== #

BASE_MAIN_LINKS = [
    'https://www.nasdaq.com/news-and-insights/markets',
    'https://www.nasdaq.com/news-and-insights/company-intel',
    'https://www.nasdaq.com/news-and-insights/technology'
]

MAX_PAGES = 5  # Maximum number of pages to iterate through per main link

NASDAQ_DATA_DIR = '../../data/nasdaq_data/urls'
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

# Configure logging
logging.basicConfig(
    filename='nasdaq_scraper.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

# ========================== Helper Functions ========================== #

def save_urls(new_urls, directory):
    """Save the new URLs to a JSONL file with the current date."""
    current_date = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    filename = os.path.join(directory, f'urls_{current_date}.jsonl')
    with open(filename, 'w', encoding='utf-8') as f:
        for url in new_urls:
            json_line = json.dumps({'url': url})
            f.write(f"{json_line}\n")
    logging.info(f"Saved {len(new_urls)} new URLs to {filename}")
    print(f"Saved {len(new_urls)} new URLs to {filename} at {current_date}")

def setup_driver(headless=True):
    """Initialize and return a Selenium WebDriver with realistic settings."""
    options = Options()

    # Randomly select a User-Agent
    user_agent = random.choice(USER_AGENTS)
    options.add_argument(f'user-agent={user_agent}')

    if headless:
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')

    options.add_argument('--no-sandbox')
    options.add_argument("--window-size=1920,1080")
    options.add_argument('--disable-blink-features=AutomationControlled')  # Remove automation flags
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)

    # Initialize WebDriver with Service
    service = Service(ChromeDriverManager().install())
    try:
        driver = webdriver.Chrome(service=service, options=options)
    except WebDriverException as e:
        logging.error(f"Error initializing ChromeDriver: {e}")
        raise e

    # Additional steps to make Selenium more stealthy
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

def scroll_to_pagination(driver):
    """Scroll the page in small increments to mimic human scrolling."""
    scroll_height = driver.execute_script("return document.body.scrollHeight")
    current_position = 0
    scroll_increment = 300  # Pixels to scroll each time

    while current_position < scroll_height:
        driver.execute_script(f"window.scrollBy(0, {scroll_increment});")
        current_position += scroll_increment
        human_like_delay()
        scroll_height = driver.execute_script("return document.body.scrollHeight")

        # Check if the element with ID 'recordsPerpage' is present
        try:
            element = driver.find_element(By.ID, "recordsPerpage")
            if element.is_displayed():
                # print("Element #recordsPerpage is present.")
                break  # Stop scrolling once the element is found
        except NoSuchElementException:
            print("Element #recordsPerpage not found yet. Continuing to scroll...")


    # Scroll to the dropdown again to confirm
    try:
        dropdown = driver.find_element(By.CSS_SELECTOR, '#recordsPerpage')
        driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", dropdown)
        # print("Centered the dropdown.")
    except NoSuchElementException:
        logging.warning("Dropdown not found during scrolling.")

    human_like_delay()
    logging.info("Completed human-like scrolling.")

def close_popups(driver):
    """Close any pop-up dialogs if present."""
    try:
        # Example: Close cookie consent pop-up
        consent_buttons = driver.find_elements(By.XPATH,
                                               "//button[contains(text(), 'Accept') or contains(text(), 'agree') or contains(text(), 'Agree') or contains(text(), 'accept')]")
        for button in consent_buttons:
            if button.is_displayed():
                button.click()
                logging.info("Closed a pop-up dialog.")
                human_like_delay(1, 2)
    except Exception as e:
        logging.warning(f"No pop-ups to close or error occurred: {e}")

def fetch_urls_from_page(driver):
    """Extract URLs from the current page."""
    urls = set()
    try:
        # Wait until the article links are present
        human_like_delay(5,15)

        # Adjust the selector based on the actual HTML structure
        elements = driver.find_elements(By.CSS_SELECTOR, 'a.jupiter22-c-article-list__item_title_wrapper')
        logging.info(f"Found {len(elements)} article links on the page.")
        # print(f"Found {len(elements)} article links on the page.")

        for elem in elements:
            href = elem.get_attribute('href')
            if href:
                if href.startswith('/'):
                    href = f"https://www.nasdaq.com{href}"
                urls.add(href)

    except TimeoutException:
        logging.error("Timeout while waiting for article links to load.")
        print("Timeout while waiting for article links to load.")
    except Exception as e:
        logging.error(f"Error fetching URLs: {e}")
        print(f"Error fetching URLs: {e}")
    return urls

def select_rows_per_page(driver):
    """
    Interact with the pagination dropdown to select 100 articles per page.
    """
    try:
        # Wait for the dropdown to be present
        dropdown = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, '#recordsPerpage'))
        )

        # Scroll to the dropdown again to confirm
        try:
            dropdown = driver.find_element(By.CSS_SELECTOR, '#recordsPerpage')
            driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", dropdown)
            # print("Centered the dropdown.")
        except NoSuchElementException:
            logging.warning("Dropdown not found during scrolling.")

        human_like_delay(2, 4)

        # Click the dropdown to open it
        dropdown.click()
        logging.info("Clicked the pagination dropdown.")
        # print("Clicked the pagination dropdown.")

        human_like_delay(1, 2)

        # Select the 100 articles per page option
        # Update the selector based on actual option value or text
        option = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//li[contains(text(), '100')]"))
        )
        option.click()
        logging.info("Selected the option for 100 articles per page.")
        # print("Selected the option for 100 articles per page.")

        # Wait for the page to reload or update
        WebDriverWait(driver, 15).until(
            EC.staleness_of(option)  # Wait until the dropdown is stale, indicating a page reload
        )
        logging.info("Page reloaded after selecting 100 articles per page.")
        # print("Page reloaded after selecting 100 articles per page.")

        scroll_to_pagination(driver)

        # Wait for the articles to load
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, '.jupiter22-c-article-list__item_title_wrapper'))
        )
        human_like_delay(2, 4)

        # Scroll to the dropdown again to confirm
        try:
            dropdown = driver.find_element(By.CSS_SELECTOR, '#recordsPerpage')
            driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", dropdown)
            # print("Centered the dropdown.")
        except NoSuchElementException:
            logging.warning("Dropdown not found during scrolling.")

        logging.info("Scrolled to the pagination dropdown after page reload.")
        # print("Scrolled to the pagination dropdown after page reload.")

    except TimeoutException:
        logging.error("Timeout while selecting rows per page.")
        print("Timeout while selecting rows per page.")
    except Exception as e:
        logging.error(f"Error selecting rows per page: {e}")
        print(f"Error selecting rows per page: {e}")

def click_next_page(page, driver):
    """
    Click the 'Next' button to navigate to the next page.
    Returns True if navigation was successful, False otherwise.
    """
    try:
        # Scroll to the dropdown again to confirm
        try:
            dropdown = driver.find_element(By.CSS_SELECTOR, '#recordsPerpage')
            driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", dropdown)
            # print("Centered the dropdown.")
        except NoSuchElementException:
            logging.warning("Dropdown not found during scrolling.")

        # Set next button
        next_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, f'div.pagination-pages-wrapper > div > button:nth-child({page})'))
        )

        next_button.click()
        logging.info(f"Clicked the '{page}' page button. Reloading...")
        # print(f"Clicked the '{page}' page button. Reloading...")

        # Scroll the page like a human
        scroll_to_pagination(driver)

        # Wait for the page to load
        WebDriverWait(driver, 15).until(
            EC.staleness_of(next_button)
        )
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, '.jupiter22-c-article-list__item_title_wrapper'))
        )
        human_like_delay(2, 4)
        return True
    except (TimeoutException, NoSuchElementException, ElementClickInterceptedException) as e:
        logging.warning(f"Could not navigate to the next page: {e}")
        print(f"Could not navigate to the next page: {e}")
        return False

# ========================== Main Scraping Logic ========================== #

def scrape_nasdaq_urls():
    # Ensure the data directory exists
    os.makedirs(NASDAQ_DATA_DIR, exist_ok=True)

    # # Load previously fetched URLs to avoid duplicates
    # previous_urls = load_previous_urls(NASDAQ_DATA_DIR)
    # logging.info(f"Loaded {len(previous_urls)} previous URLs.")
    # print(f"Loaded {len(previous_urls)} previous URLs.")

    new_urls = set()

    # Setup Selenium WebDriver
    try:
        driver = setup_driver(headless=HEADLESS)
    except WebDriverException:
        logging.critical("Failed to initialize WebDriver. Exiting script.")
        return

    try:
        for base_main_link in BASE_MAIN_LINKS:
            # print(f"\nProcessing main link: {base_main_link}")
            logging.info(f"Processing main link: {base_main_link}")

            try:
                # Load the main link
                driver.get(base_main_link)
                logging.info(f"Loaded main link: {base_main_link}")
                # print(f"Loaded main link: {base_main_link}")
            except Exception as e:
                logging.error(f"Error loading main link {base_main_link}: {e}")
                print(f"Error loading main link {base_main_link}: {e}")
                continue

            # Human-like delay after page load
            human_like_delay(2, 4)

            # Close any pop-ups that might appear
            close_popups(driver)

            # Human-like delay after closing pop-ups
            human_like_delay(2, 4)

            # Scroll the page like a human
            scroll_to_pagination(driver)

            # Human-like delay before interacting with the dropdown
            human_like_delay(1, 3)

            # Interact with the pagination dropdown to select 100 articles per page
            select_rows_per_page(driver)

            # Human-like delay after selecting the dropdown
            human_like_delay(2, 4)

            # Fetch URLs from the first page
            urls = fetch_urls_from_page(driver)
            logging.info(f"Found {len(urls)} URLs on page 1 of {base_main_link}.")
            new_urls.update(url for url in urls)

            # Iterate through the next pages (2 to MAX_PAGES)
            for current_page in range(2, MAX_PAGES + 1):
                # print(f"Navigating to page {current_page} of {base_main_link}")
                logging.info(f"Navigating to page {current_page} of {base_main_link}")

                success = click_next_page(current_page, driver)
                if not success:
                    logging.warning(f"Stopping pagination for {base_main_link} due to navigation failure.")
                    break  # Exit pagination loop if unable to navigate further

                # Fetch URLs from the current page
                urls = fetch_urls_from_page(driver)
                logging.info(f"Found {len(urls)} URLs on page {current_page} of {base_main_link}.")
                new_urls.update(url for url in urls)

    except Exception as e:
        logging.critical(f"An unexpected error occurred: {e}")
        print(f"An unexpected error occurred: {e}")

    finally:
        driver.quit()
        logging.info("WebDriver has been closed.")
        print("WebDriver has been closed.")

    # Save the new URLs if any were found
    if new_urls:
        save_urls(sorted(new_urls), NASDAQ_DATA_DIR)
        return list(new_urls)
    else:
        logging.info("No new URLs found.")
        print("No new URLs found.")
