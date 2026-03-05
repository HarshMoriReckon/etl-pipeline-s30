from pathlib import Path

BASE_DIR = Path(__file__).parent

DB_PATH = BASE_DIR / "database" / "S30 ETL Assignment.db"
SQL_CSV_OUTPUT_PATH = BASE_DIR / "output" / "sql_result.csv"
PANDAS_CSV_OUTPUT_PATH = BASE_DIR / "output" / "pandas_result.csv"
CSV_DELIMITER = ';'