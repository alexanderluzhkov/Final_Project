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
from selenium.common.exceptions import TimeoutException
import random

# Function to save the article to the PostgreSQL database
def save_article_to_db(cursor, article_data):
    try:
        # Insert article data
        cursor.execute('''
            INSERT INTO articles (url, source, title, content)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (url) DO NOTHING
        ''', (article_data['url'], article_data['source'], article_data['title'], article_data['content']))
    except Exception as e:
        print(f"Database error during insert: {e}")
        # Rollback the transaction to avoid blocking further inserts
        cursor.connection.rollback()

# Function to resolve tracking URLs
def resolve_url(url):
    try:
        session = requests.Session()
        resp = session.head(url, allow_redirects=True, timeout=10)
        final_url = resp.url
        return final_url
    except Exception as e:
        print(f"Failed to resolve URL {url}: {e}")
        return url  # Return the original URL if resolving fails

# Function to scrape Medium articles (unchanged)
def scrape_medium_article(driver, url, retries=3):
    article_data = {'url': url, 'source': 'Medium', 'title': None, 'content': None}
    try:
        # Resolve the tracking URL
        resolved_url = resolve_url(url)
        if resolved_url != url:
            print(f"Resolved URL: {resolved_url}")
        else:
            print(f"Could not resolve URL: {url}")

        # Random wait before accessing the URL
        time.sleep(random.uniform(2, 5))
        driver.get(resolved_url)

        # Wait for the article element to be present
        try:
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, 'article')))
        except TimeoutException:
            print(f"Timeout while waiting for the article to load at {resolved_url}")
            return None

        # Check for login prompts or paywalls
        if "Get unlimited access" in driver.page_source:
            print(f"Article at {resolved_url} requires login or subscription.")
            return None

        # Extract the article content using a more specific selector
        article_element = driver.find_element(By.XPATH, '//article')
        article_text = article_element.text

        # Extract the article title
        title_element = driver.find_element(By.TAG_NAME, 'h1')
        article_title = title_element.text

        article_data['url'] = resolved_url
        article_data['title'] = article_title
        article_data['content'] = article_text

        print(f"Medium article '{article_title}' successfully scraped.")
        return article_data

    except Exception as e:
        print(f"An error occurred while scraping Medium article {url}: {e}")
        traceback.print_exc()
        return None

# Function to scrape MIT Technology Review articles
def scrape_mit_article(driver, url):
    article_data = {'url': url, 'source': 'MIT Technology Review', 'title': None, 'content': None}
    try:
        # Resolve the tracking URL to get the actual article URL
        resolved_url = resolve_url(url)
        if resolved_url != url:
            print(f"Resolved URL: {resolved_url}")
        else:
            print(f"Could not resolve URL: {url}")

        driver.get(resolved_url)
        time.sleep(5)

        # Extract the article content
        article_element = driver.find_element(By.TAG_NAME, 'article')
        article_text = article_element.text

        # Extract the article title
        title_element = driver.find_element(By.TAG_NAME, 'h1')
        article_title = title_element.text

        article_data['url'] = resolved_url  # Update to the actual URL
        article_data['title'] = article_title
        article_data['content'] = article_text

        print(f"MIT article '{article_title}' successfully scraped.")
        return article_data

    except Exception as e:
        print(f"An error occurred while scraping MIT article {url}: {e}")
        return None

# Set up Chrome options
options = Options()
options.add_argument('--headless')  # Run Chrome in headless mode
options.add_argument('--disable-gpu')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')

# Optionally, add a user-agent to mimic a real browser
user_agent = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko)"
)
options.add_argument(f'user-agent={user_agent}')

# Initialize the WebDriver
driver = webdriver.Chrome(options=options)

# Define the database connection parameters
db_params = {
    'dbname': 'Articles_Medium',
    'user': 'postgres',
    'password': 'Begemotik',
    'host': 'localhost',
    'port': '5432'
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
            source TEXT,
            title TEXT,
            content TEXT
        )
    ''')

    # Fetch URLs and sources from the Google Sheet
    spreadsheet_id = '13bts6J26AhmMlPI2cK1wHVPilAXdTrmHJeFjPG94mio'
    csv_url = f'https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv'

    response = requests.get(csv_url)
    if response.status_code == 200:
        csv_data = response.content.decode('utf-8')
        f = StringIO(csv_data)
        reader = csv.reader(f)
        data_rows = [row for row in reader if row]
    else:
        print(f"Failed to fetch CSV data. Status code: {response.status_code}")
        data_rows = []

    if not data_rows:
        print("No data to process.")
    else:
        for row in data_rows:
            if len(row) < 2:
                print(f"Invalid row format: {row}")
                continue
            url, source = row[0], row[1]

            # Decide which scraping function to use based on the source
            if source == 'Medium':
                article_data = scrape_medium_article(driver, url)
            elif source == 'MIT Technology Review':
                article_data = scrape_mit_article(driver, url)
            else:
                print(f"Unknown source '{source}' for URL {url}. Skipping.")
                continue

            if article_data:
                # Save the article to the database
                save_article_to_db(cursor, article_data)
                # Commit after each successful insert
                conn.commit()
            else:
                # Rollback if scraping failed
                conn.rollback()

        print("All articles have been processed.")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    cursor.close()
    conn.close()
    driver.quit()
