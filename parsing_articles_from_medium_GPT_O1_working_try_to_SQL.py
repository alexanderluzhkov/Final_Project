import psycopg2
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import time

# Function to save the article to the PostgreSQL database
def save_article_to_db(conn_params, article_data):
    try:
        # Connect to PostgreSQL database
        conn = psycopg2.connect(**conn_params)
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

        # Insert article data
        cursor.execute('''
            INSERT INTO articles (url, title, content)
            VALUES (%s, %s, %s)
            ON CONFLICT (url) DO NOTHING
        ''', (article_data['url'], article_data['title'], article_data['content']))

        # Commit changes and close connection
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Database error: {e}")

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

try:
    # Navigate to the webpage
    url = 'https://medium.com/@towardsdatascience/' \
          'the-data-all-around-us-from-sports-to-household-management-9ce3f2f97e4c'
    driver.get(url)

    # Wait for the page to fully load
    time.sleep(5)

    # Scroll to load dynamic content
    last_height = driver.execute_script("return document.body.scrollHeight")
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)  # Wait for new content to load
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

    # Extract the article content
    article_element = driver.find_element(By.TAG_NAME, 'article')
    article_text = article_element.text

    # Extract the article title
    title_element = driver.find_element(By.TAG_NAME, 'h1')
    article_title = title_element.text

    # Prepare article data
    article_data = {
        'url': url,
        'title': article_title,
        'content': article_text
    }

    # Define the database connection parameters
    db_params = {
        'dbname': 'Articles_Medium',
        'user': 'postgres',
        'password': 'Begemotik',
        'host': 'localhost',
        'port': '5432'
    }

    # Save the article to the database
    save_article_to_db(db_params, article_data)

    print("Article successfully saved to the PostgreSQL database.")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    driver.quit()
