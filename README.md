## Airflow Orchestration

the same ETL pipeline was orchestrated using Apache Airflow 3.x.

Two DAGs were built:
- `etl_sql_solution` — uses `SQLExecuteQueryOperator` to run the SQL query, results passed to a load task via XCom
- `etl_pandas_solution` — uses `@task` functions for extract, transform, and load steps with Pandas

🎥 [Watch the walkthrough video](https://drive.google.com/file/d/1WqRok9AD9mVSmjOIDokGyX8qaTLZJj_1/view?usp=sharing)
