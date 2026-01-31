import duckdb
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent

# Dossier warehouse
warehouse_dir = BASE / "warehouse"
warehouse_dir.mkdir(exist_ok=True)

# Chemin de la base DuckDB
db_path = warehouse_dir / "dvf_warehouse.duckdb"

# Dossier curated
curated = BASE / "data_lake/curated"

# Connexion DuckDB (création automatique du fichier)
con = duckdb.connect(str(db_path))

# Sécurité : suppression si tables existent
con.execute("DROP TABLE IF EXISTS france_monthly;")
con.execute("DROP TABLE IF EXISTS top_departments;")

# Chargement des datasets CURATED
con.execute("""
    CREATE TABLE france_monthly AS
    SELECT *
    FROM read_csv_auto(?)
""", [str(curated / "dvf_france_monthly_bi.csv")])

con.execute("""
    CREATE TABLE top_departments AS
    SELECT *
    FROM read_csv_auto(?)
""", [str(curated / "dvf_top_departments_bi.csv")])

# Vérification
print("Tables créées :")
print(con.execute("SHOW TABLES").fetchall())

print("\nNombre de lignes :")
print("france_monthly :", con.execute("SELECT COUNT(*) FROM france_monthly").fetchone())
print("top_departments :", con.execute("SELECT COUNT(*) FROM top_departments").fetchone())

con.close()

print("\nWAREHOUSE DUCKDB OK")
