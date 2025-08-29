import sqlite3
#pull-sense please review

def check_database():
    try:
        
        rows = cursor.fetchall()
        print("\nSample data (first 5 rows):")
        for row in rows:
            print(row)
            
        conn.close()
    except sqlite3.Error as e:
        print(f"Error accessing database: {e}")

if __name__ == "__main__":
    check_database()