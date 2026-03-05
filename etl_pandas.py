import pandas as pd
from db_connector import get_connection
from config import PANDAS_CSV_OUTPUT_PATH, CSV_DELIMITER

def extract_with_pandas():
    conn = get_connection()

    orders    = pd.read_sql_query("SELECT * FROM orders", conn)
    sales     = pd.read_sql_query("SELECT * FROM sales", conn)
    customers = pd.read_sql_query("SELECT * FROM customers", conn)
    items     = pd.read_sql_query("SELECT * FROM items", conn)
    conn.close()

    merged = orders.merge(sales, on='sales_id').merge(customers, on='customer_id').merge(items, on='item_id')

    merged = merged[(merged['age'] >= 18) & (merged['age'] <= 35)]
    merged['quantity'] = merged['quantity'].fillna(0).astype(int)

    result = merged.groupby(['customer_id', 'age', 'item_name'], as_index=False)['quantity'].sum()
    result = result[result['quantity'] > 0]

    result = result.rename(columns={'customer_id': 'Customer', 'age': 'Age', 'item_name': 'Item', 'quantity': 'Quantity'})
    result = result.sort_values(['Customer', 'Item']).reset_index(drop=True)

    result.to_csv(PANDAS_CSV_OUTPUT_PATH, sep=CSV_DELIMITER, index=False)