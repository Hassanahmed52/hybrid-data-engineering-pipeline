import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


def init_system():
    # Adding a 10-second timeout to ensure the handshake completes
    engine = create_engine(DB_URL, connect_args={'connect_timeout': 10})

    print("Creating tables in PostgreSQL...")
    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS sales_historical;"))
        conn.execute(text("""
            CREATE TABLE sales_historical (
                transaction_id SERIAL PRIMARY KEY,
                category VARCHAR(100),
                amount DECIMAL(10, 2),
                timestamp TIMESTAMP
            );
        """))
        conn.commit()  # Ensure changes are saved

    print("Loading 15,000 records into Batch Layer...")
    df = pd.read_csv('ecommerce_transactions.csv')

    # Selecting and renaming to match our schema
    batch_df = df.head(15000)[['Product_Category', 'Purchase_Amount', 'Transaction_Date']]
    batch_df.columns = ['category', 'amount', 'timestamp']

    # Convert timestamp column to datetime objects
    batch_df['timestamp'] = pd.to_datetime(batch_df['timestamp'])

    # Ingesting the data
    batch_df.to_sql('sales_historical', engine, if_exists='append', index=False)
    print("Success! Batch data ingestion complete.")


if __name__ == "__main__":
    init_system()