import duckdb
import pandas as pd
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent

# PATHS

warehouse_path = BASE / "warehouse" / "dvf_warehouse.duckdb"
curated_path = BASE / "data_lake" / "curated"

# CONNECT DUCKDB

con = duckdb.connect(str(warehouse_path))

print("=== STEP 5 – DATA WAREHOUSE VALIDATION ===\n")

# VERIFY THAT ALL TABLES EXIST

tables = con.execute("SHOW TABLES").fetchall()
table_names = [t[0] for t in tables]

print("Tables found in warehouse:", table_names)

assert "france_monthly" in table_names, " Table france_monthly missing"
assert "top_departments" in table_names, " Table top_departments missing"

print("✔ All required tables exist\n")

# 2️⃣ VERIFY TABLES CONTAIN DATA

france_count = con.execute(
    "SELECT COUNT(*) FROM france_monthly"
).fetchone()[0]

dept_count = con.execute(
    "SELECT COUNT(*) FROM top_departments"
).fetchone()[0]

print(f"france_monthly rows: {france_count}")
print(f"top_departments rows: {dept_count}")

assert france_count > 0, " france_monthly is empty"
assert dept_count > 0, " top_departments is empty"

print("✔ Tables contain data\n")

# VERIFY ROW COUNTS VS SOURCE FILES
# Load source CSVs
france_csv = pd.read_csv(curated_path / "dvf_france_monthly_bi.csv")
dept_csv = pd.read_csv(curated_path / "dvf_top_departments_bi.csv")

print("Row count comparison:")
print("france_monthly → CSV:", len(france_csv), "| DuckDB:", france_count)
print("top_departments → CSV:", len(dept_csv), "| DuckDB:", dept_count)

assert len(france_csv) == france_count, " Row count mismatch for france_monthly"
assert len(dept_csv) == dept_count, " Row count mismatch for top_departments"

print("✔ Row counts are consistent with source files\n")

con.close()

print("DATA WAREHOUSE VALIDATION OK ")
