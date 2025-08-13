import pandas as pd
import sqlite3

def process_data():
    # Load the CSV data into a pandas DataFrame
    file_path ="src/data/transactions.csv"
    df = pd.read_csv(file_path, encoding="utf-8")

    # Drop rows with any missing values
    df.dropna(inplace=True)

    # Convert TransactionDate to datetime
    df["TransactionDate"] = pd.to_datetime(df["TransactionDate"])

    # Rename columns to match database schema (optional if already matching)
    df.rename(columns={
        "TransactionID": "transaction_id",
        "CustomerID": "customer_id",
        "Product": "product",
        "Amount": "amount",
        "TransactionDate": "TransactionDate",
        "PaymentMethod": "PaymentMethod",
        "City": "City",
        "Category": "Category"
    }, inplace=True)

    # Connect to SQLite database
    conn = sqlite3.connect("src/data/transactions.db")
    cursor = conn.cursor()

    # Create the transactions table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id INTEGER PRIMARY KEY,
            customer_id TEXT,
            product TEXT,
            amount REAL,
            TransactionDate TEXT,
            PaymentMethod TEXT,
            City TEXT,
            Category TEXT
        )
    """)

    # Insert data into the SQLite table (replacing existing)
    df.to_sql("transactions", conn, if_exists="replace", index=False)

    # Top 5 Most Sold Products
    print("\n📊 Top 5 Most Sold Products:")
    cursor.execute("""
        SELECT product, COUNT(*) AS sales_count
        FROM transactions
        GROUP BY product
        ORDER BY sales_count DESC
        LIMIT 5
    """)
    print(cursor.fetchall())

    # Monthly Revenue Trend
    print("\n📈 Monthly Revenue Trend:")
    cursor.execute("""
        SELECT strftime('%Y-%m', TransactionDate) AS month, SUM(amount) AS total_revenue
        FROM transactions
        GROUP BY month
        ORDER BY month
    """)
    print(cursor.fetchall())

    # Payment Method Popularity
    print("\n💳 Payment Method Popularity:")
    cursor.execute("""
        SELECT PaymentMethod, COUNT(*) AS usage_count
        FROM transactions
        GROUP BY PaymentMethod
        ORDER BY usage_count DESC
    """)
    print(cursor.fetchall())

    # Top 5 Cities with Most Transactions
    print("\n🏙 Top 5 Cities with Most Transactions:")
    cursor.execute("""
        SELECT City, COUNT(*) AS transaction_count
        FROM transactions
        GROUP BY City
        ORDER BY transaction_count DESC
        LIMIT 5
    """)
    print(cursor.fetchall())

    # Top 5 High-Spending Customers
    print("\n💰 Top 5 High-Spending Customers:")
    cursor.execute("""
        SELECT customer_id, SUM(amount) AS total_spent
        FROM transactions
        GROUP BY customer_id
        ORDER BY total_spent DESC
        LIMIT 5
    """)
    print(cursor.fetchall())

    # Hadoop vs Spark Related Product Sales
    print("\n🔍 Hadoop vs Spark Related Product Sales:")
    cursor.execute("""
        SELECT 
            CASE 
                WHEN product LIKE '%Hadoop%' THEN 'Hadoop'
                WHEN product LIKE '%Spark%' THEN 'Spark'
                ELSE 'Other'
            END AS category,
            COUNT(*) AS sales_count
        FROM transactions
        WHERE product LIKE '%Hadoop%' OR product LIKE '%Spark%'
        GROUP BY category
    """)
    print(cursor.fetchall())

    # Top Spending Customers in Each City (without ROW_NUMBER)
    print("\n🌆 Top Spending Customers in Each City:")
    cursor.execute("""
        SELECT t1.City, t1.customer_id, t1.total_spent
        FROM (
            SELECT City, customer_id, SUM(amount) AS total_spent
            FROM transactions
            GROUP BY City, customer_id
        ) t1
        JOIN (
            SELECT City, MAX(total_spent) AS max_spent
            FROM (
                SELECT City, customer_id, SUM(amount) AS total_spent
                FROM transactions
                GROUP BY City, customer_id
            )
            GROUP BY City
        ) t2
        ON t1.City = t2.City AND t1.total_spent = t2.max_spent
    """)
    print(cursor.fetchall())

    # Finalize
    conn.commit()
    conn.close()
    print("\n✅ Data Processing & Analysis Completed Successfully!")

if __name__ == "__main__":
    process_data()