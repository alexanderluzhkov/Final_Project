import os
import psycopg2
import openai
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# OpenAI API Key
openai.api_key = os.getenv('OPENAI_API_KEY')

# Database connection parameters
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT', 5432)
db_name = os.getenv('DB_NAME')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')

# Connect to the PostgreSQL database
try:
    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_password
    )
    conn.autocommit = True
    cursor = conn.cursor()
except Exception as e:
    print("Error connecting to the database:", e)
    exit()

# Fetch all records from the 'all_summaries' table
cursor.execute("SELECT id, title, source, date, summary FROM all_summaries;")
records = cursor.fetchall()

# The prompt template (unchanged)
prompt_template = """
You will be provided with **one article summary at a time**. For each article summary, please do the following:

1. **Determine Relevance**: Decide whether the article is relevant to our campaign's topic and objectives. Answer with **"Yes"** or **"No"**.
2. **Provide a Brief Explanation**: If relevant, briefly explain how the article aligns with the campaign's topic and objectives. If not, briefly explain why it does not. Please keep your explanation concise (1-2 sentences).

**Campaign Details:**

- **Topic of the Campaign**: The influence of AI on human lives.
- **Target Audience**: Non-IT professionals.
- **Objectives**:
  - To show the influence of AI on the job market.
  - To show threats and opportunities of AI for IT and Non-IT people.
  - To introduce new models and their features.
  - To show the influence of AI on economics in the world, individual countries, and industries.

**Output Format:**

For each article summary, please provide:

- **Relevant**: Yes/No
- **Explanation**: [Your brief explanation here.]

**Please evaluate the following article summary:**

\"\"\"
{summary}
\"\"\"
"""

# Process each record
for record in records:
    article_id = record[0]
    title = record[1]
    source = record[2]
    date = record[3]
    summary = record[4]

    # Format the prompt with the article summary
    prompt = prompt_template.format(summary=summary)

    # Call OpenAI API
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=500,
            temperature=0
        )

        # Extract the assistant's reply
        if response and 'choices' in response and len(response['choices']) > 0:
            assistant_reply = response['choices'][0]['message']['content']
        else:
            print(f"Empty or invalid response from OpenAI for article ID {article_id}.")
            continue

        # Parse the assistant's reply to get relevance and explanation
        lines = assistant_reply.strip().split('\n')
        relevance = ''
        explanation = ''
        for line in lines:
            if '- **Relevant**:' in line:
                relevance = line.split(':', 1)[1].strip()
            elif '- **Explanation**:' in line:
                explanation = line.split(':', 1)[1].strip()

        # Check if relevance and explanation were parsed correctly
        if not relevance or not explanation:
            print(f"Failed to parse relevance or explanation for article ID {article_id}. Reply: {assistant_reply}")
            continue

        # Insert the data into 'all_relevance_4o' table
        insert_query = """
        INSERT INTO all_relevance_4o (id, title, source, date, relevance, explanation)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE
        SET relevance = EXCLUDED.relevance, explanation = EXCLUDED.explanation;
        """
        cursor.execute(insert_query, (article_id, title, source, date, relevance, explanation))

    except Exception as e:
        print(f"Error processing article ID {article_id}: {e}")
        continue

# Close the database connection
cursor.close()
conn.close()

print("Processing completed successfully.")