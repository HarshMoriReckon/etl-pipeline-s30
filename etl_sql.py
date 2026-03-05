from db_connector import get_connection
import csv
from config import SQL_CSV_OUTPUT_PATH, CSV_DELIMITER

def extract_with_sql():
    query = """
        SELECT
            s.customer_id,
            c.age,
            i.item_name,
            CAST(ROUND(SUM(o.quantity), 0) AS INTEGER) AS quantity
        FROM orders AS o
            LEFT JOIN sales     AS s ON o.sales_id    = s.sales_id
            LEFT JOIN customers AS c ON s.customer_id = c.customer_id
            LEFT JOIN items     AS i ON o.item_id     = i.item_id
        WHERE c.age BETWEEN 18 AND 35
        GROUP BY s.customer_id, c.age, i.item_id
        HAVING SUM(o.quantity) > 0
        ORDER BY s.customer_id, i.item_name
    """

    conn = get_connection()
    cursor = conn.cursor()
    rows = cursor.execute(query).fetchall()
    conn.close()

    with open(SQL_CSV_OUTPUT_PATH, mode='w', newline='') as f:
        writer = csv.writer(f, delimiter=CSV_DELIMITER)
        writer.writerow(["Customer", "Age", "Item", "Quantity"])
        for row in rows:
            writer.writerow(row)