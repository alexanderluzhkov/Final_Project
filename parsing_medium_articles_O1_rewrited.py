import os
import psycopg2
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import time
import requests
import csv
from io import StringIO
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
import random
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Function to save the article to the PostgreSQL database
def save_article_to_db(cursor, article_data):
    try:
        cursor.execute('''
            INSERT INTO articles (url, title, content)
            VALUES (%s, %s, %s)
            ON CONFLICT (url) DO NOTHING
        ''', (article_data['url'], article_data['title'], article_data['content']))
    except Exception as e:
        print(f"Database error during insert: {e}")
        cursor.connection.rollback()

# Function to resolve tracking URLs
def resolve_url(url):
    try:
        session = requests.Session()
        resp = session.head(url, allow_redirects=True, timeout=10)
        return resp.url
    except Exception as e:
        print(f"Failed to resolve URL {url}: {e}")
        return url

# Function to scrape Medium articles
def scrape_medium_article(driver, url):
    article_data = {'url': url, 'title': None, 'content': None}
    try:
        resolved_url = resolve_url(url)
        print(f"Resolved URL: {resolved_url}")

        time.sleep(random.uniform(2, 5))
        driver.get(resolved_url)

        # Scroll to bottom to trigger any lazy-loaded content
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)

        # Wait for the article to be visible
        try:
            WebDriverWait(driver, 30).until(
                EC.visibility_of_element_located((By.TAG_NAME, 'article'))
            )
        except TimeoutException:
            print(f"Timeout while waiting for the article to load at {resolved_url}")
            return None

        # Check for paywall or login requirement
        if "Get unlimited access" in driver.page_source or "Become a member" in driver.page_source:
            print(f"Article at {resolved_url} requires login or subscription.")
            return None

        # Retry logic for stale elements
        for attempt in range(3):
            try:
                # Re-fetch elements
                article_element = driver.find_element(By.TAG_NAME, 'article')
                article_text = article_element.text

                title_element = driver.find_element(By.TAG_NAME, 'h1')
                article_title = title_element.text

                article_data['url'] = resolved_url
                article_data['title'] = article_title
                article_data['content'] = article_text

                print(f"Medium article '{article_title}' successfully scraped.")
                return article_data

            except StaleElementReferenceException:
                if attempt < 2:
                    print(f"StaleElementReferenceException encountered. Retrying ({attempt + 1}/3)...")
                    time.sleep(2)
                    continue
                else:
                    print(f"Failed after multiple retries due to stale element.")
                    return None
            except Exception as e:
                print(f"An error occurred while scraping Medium article {url}: {e}")
                return None

    except Exception as e:
        print(f"An error occurred while scraping Medium article {url}: {e}")
        return None


# Set up Chrome options
options = Options()
options.add_argument('--headless')
options.add_argument('--disable-gpu')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')

user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"
options.add_argument(f'user-agent={user_agent}')

# Suppress irrelevant error messages
options.add_argument('--log-level=3')

# Initialize the WebDriver
driver = webdriver.Chrome(options=options)

# Define the database connection parameters from environment variables
db_params = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

try:
    # Connect to PostgreSQL database
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    # Create the articles table if it doesn't exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS articles (
            id SERIAL PRIMARY KEY,
            url TEXT UNIQUE,
            title TEXT,
            content TEXT
        )
    ''')

    # Fetch URLs from the Google Sheet
    spreadsheet_id = '13bts6J26AhmMlPI2cK1wHVPilAXdTrmHJeFjPG94mio'
    csv_url = f'https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv'

    response = requests.get(csv_url)
    if response.status_code == 200:
        csv_data = response.content.decode('utf-8')
        f = StringIO(csv_data)
        reader = csv.reader(f)
        urls = [row[0] for row in reader if row and 'medium.com' in row[0]]
    else:
        print(f"Failed to fetch CSV data. Status code: {response.status_code}")
        urls = []

    if not urls:
        print("No Medium URLs to process.")
    else:
        for url in urls:
            article_data = scrape_medium_article(driver, url)
            if article_data:
                save_article_to_db(cursor, article_data)
                conn.commit()
            else:
                conn.rollback()

        print("All Medium articles have been processed.")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    cursor.close()
    conn.close()
    driver.quit()
