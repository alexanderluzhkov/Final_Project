import psycopg2
import openai
import os
from datetime import datetime
from dotenv import load_dotenv  # Import the function to load the .env file

# Load environment variables from the .env file
load_dotenv()

# Function to retrieve articles from the database
def get_articles(conn_params):
    try:
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        # Select articles from 'Medium' source and get necessary fields
        cursor.execute('SELECT id, url, title, source, content FROM articles WHERE source = %s', ('Medium',))
        articles = cursor.fetchall()
        cursor.close()
        conn.close()
        return articles
    except Exception as e:
        print(f"Database error: {e}")
        return []

# Function to save summaries to the 'Medium_summaries' table
def save_summary_to_db(conn_params, summary_data):
    try:
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()

        # Create Medium_summaries table if it doesn't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Medium_summaries (
                id SERIAL PRIMARY KEY,
                url TEXT UNIQUE,
                title TEXT,
                author TEXT,
                summary TEXT,
                source TEXT,
                date TIMESTAMP
            )
        ''')

        # Insert or update the summary for the article in Medium_summaries
        cursor.execute('''
            INSERT INTO Medium_summaries (url, title, author, summary, source, date)
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
        print(f"Database error: {e}")

# Function to summarize text using OpenAI API
def summarize_text(text):
    # Set your OpenAI API key
    openai.api_key = os.getenv("OPENAI_API_KEY")
    if not openai.api_key:
        raise ValueError("Please set your OpenAI API key in the .env file as 'OPENAI_API_KEY'")

    # Optional: Truncate text if it's too long
    max_input_tokens = 4000
    if len(text) > max_input_tokens:
        text = text[:max_input_tokens]

    # Use the OpenAI API to summarize the text
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4",  # Use "gpt-3.5-turbo" if you don't have access to GPT-4
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
    # Define the database connection parameters
    db_params = {
        'dbname': 'Articles_Medium',
        'user': 'postgres',
        'password': 'Begemotik',
        'host': 'localhost',
        'port': '5432'
    }

    # Retrieve articles from the database where source is 'Medium'
    articles = get_articles(db_params)

    for article in articles:
        article_id = article[0]
        url = article[1]
        title = article[2]
        source = article[3]
        content = article[4]
        author = 'N/A'  # Assign 'N/A' to author since it's not available
        print(f"Summarizing Article ID: {article_id}, Title: {title}")

        # Summarize the article content
        summary = summarize_text(content)
        if summary:
            # Prepare summary data with current date
            summary_data = {
                'url': url,
                'title': title,
                'author': author,
                'summary': summary,
                'source': source,
                'date': datetime.now()
            }
            # Save the summary to the Medium_summaries table
            save_summary_to_db(db_params, summary_data)
            print(f"Summary saved for Article ID: {article_id}\n")
        else:
            print(f"Failed to summarize Article ID: {article_id}\n")
