import sys
from config import DB_PATH, SQL_CSV_OUTPUT_PATH
from etl_sql import extract_with_sql
from etl_pandas import extract_with_pandas

def check_db():
    if not DB_PATH.exists():
        print(f"Database not found at: {DB_PATH}")
        sys.exit(1)

def check_output_folder():
    output_dir = SQL_CSV_OUTPUT_PATH.parent
    if not output_dir.exists():
        output_dir.mkdir(parents=True, exist_ok=True)

def main():
    check_db()
    check_output_folder()

    print("Running SQL solution...")
    extract_with_sql()
    print("SQL done.")

    print("Running Pandas solution...")
    extract_with_pandas()
    print("Pandas done.")

if __name__ == "__main__":
    main()