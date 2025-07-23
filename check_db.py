import sqlite3

def check_database():
    try:
        conn = sqlite3.connect('products.db')
        cursor = conn.cursor()
        
        # Check row count
        cursor.execute("SELECT COUNT(*) FROM products")
        count = cursor.fetchone()[0]
        print(f"Total rows in products table: {count}")
        
        # View sample data
        cursor.execute("SELECT product_id, sku_id, product_name, seller_name FROM products LIMIT 5")
        rows = cursor.fetchall()
        print("\nSample data (first 5 rows):")
        for row in rows:
            print(row)
            
        conn.close()
    except sqlite3.Error as e:
        print(f"Error accessing database: {e}")

if __name__ == "__main__":
    check_database()