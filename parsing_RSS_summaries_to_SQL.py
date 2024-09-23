import feedparser
from datetime import datetime
import psycopg2
from psycopg2 import sql
from bs4 import BeautifulSoup

# Database connection parameters
db_params = {
    'dbname': 'Articles_Medium',
    'user': 'postgres',
    'password': 'Begemotik',
    'host': 'localhost',
    'port': '5432'
}

def ensure_columns_exist(cursor):
    # List of required columns and their data types
    required_columns = {
        'author': 'TEXT'
    }
    # Check existing columns
    cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name='summaries';")
    existing_columns = [row[0] for row in cursor.fetchall()]
    # Add missing columns
    for column, data_type in required_columns.items():
        if column not in existing_columns:
            cursor.execute(sql.SQL("ALTER TABLE summaries ADD COLUMN {} {};").format(
                sql.Identifier(column),
                sql.SQL(data_type)
            ))
            print(f"Column '{column}' added to 'summaries' table.")

def parse_rss_feed(feed_url, source_name, cursor):
    # Parse the RSS feed
    feed = feedparser.parse(feed_url)

    # Check for parsing errors
    if feed.bozo:
        print(f"Error parsing feed from {source_name}: {feed.bozo_exception}")
        return

    # Iterate over the feed entries
    for entry in feed.entries:
        try:
            # Extract the article title
            title = entry.title if 'title' in entry else 'No Title'

            # Extract the article URL
            url = entry.link if 'link' in entry else 'No URL'

            # Extract the article author
            author = 'No Author'
            if 'author' in entry:
                author = entry.author
            elif 'dc:creator' in entry:
                author = entry['dc:creator']

            # Extract and clean the article summary from 'description'
            summary = 'No Summary'
            if 'description' in entry:
                summary_html = entry.description
                soup_summary = BeautifulSoup(summary_html, 'html.parser')
                summary = soup_summary.get_text()
            elif 'summary' in entry:
                summary_html = entry.summary
                soup_summary = BeautifulSoup(summary_html, 'html.parser')
                summary = soup_summary.get_text()

            # Date of parsing (current date)
            parsing_date = datetime.now()

            # Insert into the database
            cursor.execute('''
                INSERT INTO summaries (url, title, author, summary, source, date)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (url) DO NOTHING
            ''', (url, title, author, summary, source_name, parsing_date))

            print(f"Article '{title}' from {source_name} added to the database.")

        except Exception as e:
            print(f"Error processing entry from {source_name}: {e}")

def main():
    # RSS feed URLs and source names
    feeds = [
        {
            'url': 'https://www.technologyreview.com/feed/',
            'source': 'MIT Technology Review'
        },
        {
            'url': 'https://techcrunch.com/tag/artificial-intelligence/feed/',
            'source': 'TechCrunch'
        },
        {
            'url': 'https://www.aitrends.com/feed/',
            'source': 'AI Trends'
        }
    ]

    # Connect to PostgreSQL database
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        # Create the summaries table if it doesn't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS summaries (
                id SERIAL PRIMARY KEY,
                url TEXT UNIQUE,
                title TEXT,
                author TEXT,
                summary TEXT,
                source TEXT,
                date TIMESTAMP
            )
        ''')
        conn.commit()

        # Ensure required columns exist
        ensure_columns_exist(cursor)
        conn.commit()

        # Parse each RSS feed
        for feed_info in feeds:
            print(f"Processing feed from {feed_info['source']}")
            parse_rss_feed(feed_info['url'], feed_info['source'], cursor)
            conn.commit()

        print("All feeds have been processed.")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
