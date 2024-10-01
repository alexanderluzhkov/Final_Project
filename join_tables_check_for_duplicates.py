import os
from dotenv import load_dotenv
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

# Get database credentials from environment variables
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')

# Construct the database URL
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"

# Create the engine
engine = sa.create_engine(DATABASE_URL)

# Create a session factory
Session = sessionmaker(bind=engine)

def update_all_summaries():
    try:
        with Session() as session:
            # Update from summaries table
            summaries_result = session.execute(
                sa.text("""
                INSERT INTO all_summaries (url, title, author, summary, source, date, origin_table, last_updated)
                SELECT s.url, s.title, s.author, s.summary, s.source, s.date, 'summaries' AS origin_table, :current_time
                FROM summaries s
                WHERE NOT EXISTS (
                    SELECT 1 FROM all_summaries a WHERE a.url = s.url
                )
                """),
                {'current_time': datetime.now()}
            )

            # Update from medium_summaries table
            medium_summaries_result = session.execute(
                sa.text("""
                INSERT INTO all_summaries (url, title, author, summary, source, date, origin_table, last_updated)
                SELECT ms.url, ms.title, ms.author, ms.summary, ms.source, ms.date, 'medium_summaries' AS origin_table, :current_time
                FROM medium_summaries ms
                WHERE NOT EXISTS (
                    SELECT 1 FROM all_summaries a WHERE a.url = ms.url
                )
                """),
                {'current_time': datetime.now()}
            )

            # Commit the transaction
            session.commit()

            # Get the number of rows inserted
            summaries_inserted = summaries_result.rowcount
            medium_summaries_inserted = medium_summaries_result.rowcount

            print(f"Update completed successfully at {datetime.now()}")
            print(f"Inserted {summaries_inserted} new rows from 'summaries' table")
            print(f"Inserted {medium_summaries_inserted} new rows from 'medium_summaries' table")

    except SQLAlchemyError as e:
        print(f"An error occurred with the database: {str(e)}")
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")

if __name__ == "__main__":
    print("Starting the update process...")
    update_all_summaries()
    print("Update process completed.")