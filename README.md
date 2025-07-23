# Large CSV File Processing Pipeline

## Overview
This project provides an optimized Python script for processing a large CSV file (~350,000 rows) from a given URL, performing data cleaning and transformation, and storing the results in a SQLite database. The implementation uses Pandas for efficient data handling, chunking to manage memory, and parallel processing for performance optimization.

## Prerequisites
- Python 3.8 or higher
- Required Python packages:
  - pandas
  - sqlite3 (included with Python)
  - urllib3
  - requests
  - tqdm
- Internet connection to download the CSV file

## Setup Instructions
1. Clone or download this repository to your local machine.
2. Install the required Python packages:
   ```bash
   pip install pandas urllib3 requests tqdm 

## Execution Instructions
1. First Run python create_db.py to create database and table
2. Execute python process_data.py