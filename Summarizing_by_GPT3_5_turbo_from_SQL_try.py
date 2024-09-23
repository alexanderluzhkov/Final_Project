import psycopg2
import openai
import os
from dotenv import load_dotenv

load_dotenv()

# Function to retrieve articles from the database
def get_articles(conn_params):
    try:
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        cursor.execute('SELECT id, url, title, content FROM articles')
        articles = cursor.fetchall()
        cursor.close()
        conn.close()
        return articles
    except Exception as e:
        print(f"Database error: {e}")
        return []

# Function to save summaries to the database
def save_summary_to_db(conn_params, summary_data):
    try:
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()

        # Create summaries table if it doesn't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS summaries (
                id SERIAL PRIMARY KEY,
                article_id INTEGER UNIQUE,
                summary TEXT,
                FOREIGN KEY (article_id) REFERENCES articles (id)
            )
        ''')

        # Insert or update the summary for the article
        cursor.execute('''
            INSERT INTO summaries (article_id, summary)
            VALUES (%s, %s)
            ON CONFLICT (article_id) DO UPDATE SET summary = EXCLUDED.summary
        ''', (summary_data['article_id'], summary_data['summary']))

        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Database error: {e}")

# Function to summarize text using OpenAI API
#def summarize_text(text):
    # Set your OpenAI API key
    #openai.api_key = os.getenv("OPENAI_API_KEY")
    #if not openai.api_key:
        #raise ValueError("Please set your OpenAI API key as an environment variable named 'OPENAI_API_KEY'")
def summarize_text(text):
    # Get the OpenAI API key from environment variables
    openai.api_key = os.getenv("OPENAI_API_KEY")
    if not openai.api_key:
        raise ValueError("OpenAI API key not found in environment variables")

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

    # Retrieve articles from the database
    articles = get_articles(db_params)

    for article in articles:
        article_id = article[0]
        url = article[1]
        title = article[2]
        content = article[3]
        print(f"Summarizing Article ID: {article_id}, Title: {title}")

        # Summarize the article content
        summary = summarize_text(content)
        if summary:
            # Save the summary to the database
            summary_data = {
                'article_id': article_id,
                'summary': summary
            }
            save_summary_to_db(db_params, summary_data)
            print(f"Summary saved for Article ID: {article_id}\n")
        else:
            print(f"Failed to summarize Article ID: {article_id}\n")
