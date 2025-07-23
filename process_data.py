
import pandas as pd
import sqlite3
import requests
import gzip
import os
import io
import logging
import time
import math
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import time
from tqdm import tqdm
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


logging.basicConfig(
    filename='logs/csv_processing.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


URL = "https://tyroo-engineering-assesments.s3.us-west-2.amazonaws.com/Tyroo-dummy-data.csv.gz"
CHUNK_SIZE = 10000  
BATCH_SIZE = 100  
DB_NAME = "products.db"

DB_LOCK = threading.Lock()

def clean_data(df):
    try:
        logging.info(f"Before cleaning: {len(df)} rows in chunk")
        numeric_cols = [
            'platform_commission_rate', 'promotion_price', 'current_price',
            'product_commission_rate', 'seller_rating', 'bonus_commission_rate',
            'discount_percentage', 'rating_avg_value', 'price', 'number_of_reviews'
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

      
        df = df.fillna({
            'platform_commission_rate': 0,
            'promotion_price': 0,
            'current_price': 0,
            'product_commission_rate': 0,
            'seller_rating': 0,
            'bonus_commission_rate': 0,
            'discount_percentage': 0,
            'rating_avg_value': 0,
            'price': 0,
            'number_of_reviews': 0,
            'availability': 'unknown',
            'venture_category1_name_en': 'unknown',
            'venture_category2_name_en': 'unknown',
            'venture_category3_name_en': 'unknown'
        })

        df = df.drop_duplicates(subset=['product_id', 'sku_id'])

      
        string_cols = ['brand_name', 'seller_name', 'venture_category_name_local']
        for col in string_cols:
            if col in df.columns:
                df[col] = df[col].str.strip().str.replace('\t', ' ').str.replace('\n', ' ')

        logging.info(f"Processed chunk of {len(df)} rows")
        return df
    except Exception as e:
        logging.error(f"Error cleaning data: {str(e)}")
        return None

def store_chunk(df):
    try:
        logging.info(f"Before insertion: {len(df)} rows in chunk")
        with DB_LOCK:
            with sqlite3.connect(DB_NAME, check_same_thread=False, timeout=30) as conn:
                cursor = conn.cursor()
                cursor.execute("PRAGMA synchronous = OFF;")
                cursor.execute("PRAGMA cache_size = -20000;")
                columns = df.columns.tolist()
                placeholders = ','.join(['?' for _ in columns])
                query = f"INSERT OR IGNORE INTO products ({','.join(columns)}) VALUES ({placeholders})"
                records = [tuple(row) for row in df.to_numpy()]
                batch_size = 100
                for i in range(0, len(records), batch_size):
                    cursor.executemany(query, records[i:i + batch_size])
                conn.commit()
                logging.info(f"Stored {len(df)} rows to database")
                cursor.execute("SELECT COUNT(*) FROM products")
                count = cursor.fetchone()[0]
                logging.info(f"After insertion: {count} rows in database")
    except Exception as e:
        logging.error(f"Error storing chunk to database: {str(e)}")

def check_existing_data(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM products")
        count = cursor.fetchone()[0]
        logging.info(f"Found {count} rows in products table")
        return count > 0
    except sqlite3.Error as e:
        logging.error(f"Error checking existing data: {str(e)}")
        return False

def process_chunk(chunk):
    try:
        cleaned_df = clean_data(chunk)
        if cleaned_df is not None:
            store_chunk(cleaned_df)
    except Exception as e:
        logging.error(f"Error processing chunk: {str(e)}")

def verify_database():
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM products")
        count = cursor.fetchone()[0]
        logging.info(f"Database verification: {count} rows in products table")
        print(f"Database contains {count} rows")

        cursor.execute("SELECT product_id, sku_id, product_name, seller_name FROM products LIMIT 5")
        rows = cursor.fetchall()
        logging.info("Sample data from products table:")
        for row in rows:
            logging.info(row)
        conn.close()
    except sqlite3.Error as e:
        logging.error(f"Error verifying database: {str(e)}")



def stream_csv_chunks(url, chunk_size):
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))

    response = session.get(url, stream=True)
    response.raise_for_status()

    gzip_file = gzip.GzipFile(fileobj=response.raw)
    text_stream = io.TextIOWrapper(gzip_file, encoding='utf-8')

    return pd.read_csv(text_stream, chunksize=chunk_size, low_memory=False)


def count_rows(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM products")
    return cursor.fetchone()[0]

def main():
    start_time = time.time()
    logging.info(f"Starting CSV streaming pipeline at {datetime.now()}")

    try:
        conn = sqlite3.connect(DB_NAME, check_same_thread=False)
        logging.info(f"Connected to database: {DB_NAME}")

        chunk_iter = stream_csv_chunks(URL, CHUNK_SIZE)

        inserted_rows = 0
        with ThreadPoolExecutor(max_workers=4) as executor, tqdm(
            desc="Inserted rows", unit="rows", dynamic_ncols=True
        ) as pbar:

            futures = []

            def process_and_update(chunk):
                nonlocal inserted_rows
                cleaned_df = clean_data(chunk)
                if cleaned_df is not None and not cleaned_df.empty:
                    before_insert = count_rows(conn)
                    store_chunk(cleaned_df)
                    after_insert = count_rows(conn)
                    inserted = after_insert - before_insert
                    pbar.update(inserted)

            for chunk in chunk_iter:
                futures.append(executor.submit(process_and_update, chunk))

            for future in futures:
                future.result()

        conn.close()
        logging.info(f"Processing completed in {time.time() - start_time:.2f} seconds")
        verify_database()

    except Exception as e:
        logging.error(f"Error in main processing: {str(e)}")
        if 'conn' in locals():
            conn.close()


            
if __name__ == "__main__":
    main()