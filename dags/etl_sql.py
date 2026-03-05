# dags/etl_sql.py
import csv
import pendulum
from pathlib import Path
from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

BASE_DIR = Path("/opt/airflow")
OUT_PATH = BASE_DIR / "output" / "output_sql.csv"

QUERY = """
    SELECT
        s.customer_id,
        c.age,
        i.item_name,
        CAST(SUM(o.quantity) AS INTEGER) AS quantity
    FROM orders AS o
        LEFT JOIN sales     AS s ON o.sales_id    = s.sales_id
        LEFT JOIN customers AS c ON s.customer_id = c.customer_id
        LEFT JOIN items     AS i ON o.item_id     = i.item_id
    WHERE c.age BETWEEN 18 AND 35
    GROUP BY s.customer_id, c.age, i.item_id
    HAVING SUM(o.quantity) > 0
    ORDER BY s.customer_id, i.item_name
"""

@dag(
    dag_id="etl_sql_solution",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["etl", "sql"],
)
def etl_sql_dag():

    extract = SQLExecuteQueryOperator(
        task_id="extract_and_transform",
        conn_id="sqlite_s30",
        sql=QUERY,
        return_last=True,
    )

    @task()
    def load(rows: list):
        headers = ["Customer", "Age", "Item", "Quantity"]

        OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

        with open(OUT_PATH, "w", newline="") as f:
            writer = csv.writer(f, delimiter=";")
            writer.writerow(headers)
            writer.writerows(rows)

        print(f"Saved {len(rows)} rows to {OUT_PATH}")

    load(extract.output)

etl_sql_dag()