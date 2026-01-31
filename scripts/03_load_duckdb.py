import duckdb
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent

warehouse = BASE / "warehouse"
warehouse.mkdir(exist_ok=True)

db_path = warehouse / "dvf_warehouse.duckdb"
curated = BASE / "data_lake/curated"

# CONNECT DUCKDB

con = duckdb.connect(str(db_path))


# DROP TABLES IF EXIST

con.execute("DROP TABLE IF EXISTS france_monthly;")
con.execute("DROP TABLE IF EXISTS top_departments;")

# LOAD CURATED DATASETS

con.execute("""
    CREATE TABLE france_monthly AS
    SELECT * FROM read_csv_auto(?)
""", [str(curated / "dvf_monthly_france.csv")])

con.execute("""
    CREATE TABLE top_departments AS
    SELECT * FROM read_csv_auto(?)
""", [str(curated / "dvf_top_departments.csv")])

# VALIDATION

print("Tables dans le warehouse :")
print(con.execute("SHOW TABLES").fetchall())

print("\nNombre de lignes :")
print("france_monthly :", con.execute("SELECT COUNT(*) FROM france_monthly").fetchone())
print("top_departments :", con.execute("SELECT COUNT(*) FROM top_departments").fetchone())

con.close()

print("\nWAREHOUSE DUCKDB OK ")
