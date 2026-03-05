# dags/etl_pandas.py
import pendulum
from airflow.sdk import dag, task
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
DB_PATH  = BASE_DIR / "data"   / "S30 ETL Assignment.db"
OUT_PATH = BASE_DIR / "output" / "output_pandas.csv"

@dag(
    dag_id="etl_pandas_solution",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["etl", "pandas"],
)
def etl_pandas_dag():

    @task()
    def extract() -> dict:
        import pandas as pd
        import sqlite3
        from airflow.hooks.base import BaseHook

        conn_uri = BaseHook.get_connection("sqlite_s30").host
        conn = sqlite3.connect(conn_uri)

        def clean(df):
            return df.astype(object).where(pd.notna(df), None)

        tables = {
            "orders":    clean(pd.read_sql_query("SELECT * FROM orders", conn)).to_dict(orient="records"),
            "sales":     clean(pd.read_sql_query("SELECT * FROM sales", conn)).to_dict(orient="records"),
            "customers": clean(pd.read_sql_query("SELECT * FROM customers", conn)).to_dict(orient="records"),
            "items":     clean(pd.read_sql_query("SELECT * FROM items", conn)).to_dict(orient="records"),
        }

        conn.close()
        return tables
    
    @task()
    def transform(tables: dict) -> list:
        import pandas as pd

        orders    = pd.DataFrame(tables["orders"])
        sales     = pd.DataFrame(tables["sales"])
        customers = pd.DataFrame(tables["customers"])
        items     = pd.DataFrame(tables["items"])

        customers = customers[customers["age"].between(18, 35)]

        merged = (
            orders
            .merge(sales,     on="sales_id")
            .merge(customers, on="customer_id")
            .merge(items,     on="item_id")
        )

        merged["quantity"] = merged["quantity"].fillna(0).astype(int)

        result = (
            merged
            .groupby(["customer_id", "age", "item_name"], as_index=False)["quantity"]
            .sum()
        )

        result = result[result["quantity"] > 0]
        result = result.sort_values(["customer_id", "item_name"]).reset_index(drop=True)

        return result.values.tolist()

    @task()
    def load(rows: list):
        import csv

        headers = ["Customer", "Age", "Item", "Quantity"]
        with open(OUT_PATH, "w", newline="") as f:
            writer = csv.writer(f, delimiter=";")
            writer.writerow(headers)
            writer.writerows(rows)

    tables = extract()
    rows   = transform(tables)
    load(rows)

etl_pandas_dag()