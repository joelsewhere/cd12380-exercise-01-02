"""
create_sales_db.py

Run this script to generate a local SQLite database (sales.db) populated
with sample data that the sales_report DAG exercise query will work against.

Usage:
    python create_sales_db.py
"""

import sqlite3
import random
from datetime import date, timedelta

DB_PATH = "sales.db"
REGIONS = ["North", "South", "East", "West"]
START_DATE = date(2024, 1, 1)
END_DATE = date(2024, 12, 31)
NUM_ROWS = 500
RANDOM_SEED = 42


def generate_dates(start: date, end: date, n: int) -> list[date]:
    span = (end - start).days
    return [start + timedelta(days=random.randint(0, span)) for _ in range(n)]


def main():
    random.seed(RANDOM_SEED)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS sales")
    cursor.execute("""
        CREATE TABLE sales (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            sale_date    TEXT    NOT NULL,
            region       TEXT    NOT NULL,
            sale_amount  REAL    NOT NULL,
            tax_amount   REAL    NOT NULL
        )
    """)

    dates = generate_dates(START_DATE, END_DATE, NUM_ROWS)

    rows = []
    for sale_date in dates:
        region = random.choice(REGIONS)
        sale_amount = round(random.uniform(50.0, 2000.0), 2)
        tax_amount = round(sale_amount * 0.08, 2)
        rows.append((sale_date.isoformat(), region, sale_amount, tax_amount))

    cursor.executemany(
        "INSERT INTO sales (sale_date, region, sale_amount, tax_amount) VALUES (?, ?, ?, ?)",
        rows,
    )

    conn.commit()

    cursor.execute("SELECT COUNT(*) FROM sales")
    count = cursor.fetchone()[0]

    cursor.execute("""
        SELECT region, COUNT(*) AS num_sales, ROUND(SUM(sale_amount), 2) AS total
        FROM sales
        GROUP BY region
        ORDER BY region
    """)
    print(f"Created {DB_PATH} with {count} rows.\n")
    print(f"{'Region':<10} {'# Sales':>8} {'Total Sales':>12}")
    print("-" * 32)
    for region, num_sales, total in cursor.fetchall():
        print(f"{region:<10} {num_sales:>8} {total:>12,.2f}")

    conn.close()
    print(f"\nDatabase ready at: {DB_PATH}")


if __name__ == "__main__":
    main()