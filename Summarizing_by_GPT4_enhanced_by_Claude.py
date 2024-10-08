import psycopg2
import openai
import os
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

# Function to retrieve all articles from the database
def get_all_articles(conn_params):
    try:
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        cursor.execute('SELECT id, url, title, source, content FROM articles')
        articles = cursor.fetchall()
        cursor.close()
        conn.close()
        return articles
    except Exception as e:
        print(f"Database error in get_all_articles: {e}")
        return []

# Function to save summaries to the 'article_summaries' table
def save_summary_to_db(conn_params, summary_data):
    try:
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS medium_summaries (
                id SERIAL PRIMARY KEY,
                url TEXT UNIQUE,
                title TEXT,
                author TEXT,
                summary TEXT,
                source TEXT,
                date TIMESTAMP
            )
        ''')

        cursor.execute('''
            INSERT INTO medium_summaries (url, title, author, summary, source, date)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (url) DO UPDATE SET
                title = EXCLUDED.title,
                author = EXCLUDED.author,
                summary = EXCLUDED.summary,
                source = EXCLUDED.source,
                date = EXCLUDED.date
        ''', (summary_data['url'], summary_data['title'], summary_data['author'],
              summary_data['summary'], summary_data['source'], summary_data['date']))

        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Database error in save_summary_to_db: {e}")

# Function to summarize text using OpenAI API
def summarize_text(text):
    openai.api_key = os.getenv("OPENAI_API_KEY")
    if not openai.api_key:
        raise ValueError("OpenAI API key not found in environment variables")

    max_input_tokens = 4000
    if len(text) > max_input_tokens:
        text = text[:max_input_tokens]

    try:
        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "You are a helpful assistant that summarizes articles."},
                {"role": "user", "content": f"Please provide a concise summary of the following article:\n\n{text}"}
            ],
            max_tokens=500,
            temperature=0.5,
        )
        summary = response['choices'][0]['message']['content'].strip()
        return summary
    except Exception as e:
        print(f"OpenAI API error: {e}")
        return None

# Main script
if __name__ == "__main__":
    try:
        print("Starting article summarization process for all articles...")

        # Print API key check (first 5 characters for safety)
        api_key = os.getenv("OPENAI_API_KEY")
        print(f"API Key: {api_key[:5]}..." if api_key else "API Key not set")

        # Define the database connection parameters from environment variables
        db_params = {
            'dbname': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'host': os.getenv('DB_HOST'),
            'port': os.getenv('DB_PORT')
        }

        # Check if all database parameters are set
        if not all(db_params.values()):
            raise ValueError("One or more database connection parameters are missing from the .env file")

        articles = get_all_articles(db_params)
        print(f"Retrieved {len(articles)} articles from database")

        for i, article in enumerate(articles, 1):
            article_id, url, title, source, content = article
            author = 'N/A'  # Assign 'N/A' to author since it's not available
            print(f"Processing article {i}/{len(articles)}: ID {article_id}, Title: {title}, Source: {source}")

            summary = summarize_text(content)
            if summary:
                summary_data = {
                    'url': url,
                    'title': title,
                    'author': author,
                    'summary': summary,
                    'source': source,
                    'date': datetime.now()
                }
                save_summary_to_db(db_params, summary_data)
                print(f"Summary saved for Article ID: {article_id}")
            else:
                print(f"Failed to summarize Article ID: {article_id}")

        print("Processing complete")
    except Exception as e:
        print(f"An error occurred in the main script: {e}")