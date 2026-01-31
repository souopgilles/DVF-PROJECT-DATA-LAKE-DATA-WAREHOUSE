import duckdb
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
db_path = BASE / "warehouse" / "dvf_warehouse.duckdb"

con = duckdb.connect(str(db_path))



# Is data available for January 2026?

print("1️⃣ Data available for January 2026 ?")

query_jan_2026 = """
SELECT COUNT(*) 
FROM france_monthly
WHERE date = '2026-01-01'
"""

jan_2026_count = con.execute(query_jan_2026).fetchone()[0]

if jan_2026_count > 0:
    print("✔ Data IS available for January 2026\n")
else:
    print("❌ Data NOT available for January 2026\n")

# Latest available month

print("Latest available month")

query_latest_month = """
SELECT MAX(date) 
FROM france_monthly
"""

latest_month = con.execute(query_latest_month).fetchone()[0]
print("✔ Latest available month:", latest_month, "\n")

# Median price per square meter

print("Median price per square meter")

# a) Apartments
query_median_apartment = """
SELECT date, med_prix_m2_appartement
FROM france_monthly
ORDER BY date DESC
LIMIT 1
"""

apt_result = con.execute(query_median_apartment).fetchone()
print(f"a) Apartments → Date: {apt_result[0]}, Median price €/m²: {apt_result[1]}")

# b) Houses
query_median_house = """
SELECT date, med_prix_m2_maison
FROM france_monthly
ORDER BY date DESC
LIMIT 1
"""

house_result = con.execute(query_median_house).fetchone()
print(f"b) Houses → Date: {house_result[0]}, Median price €/m²: {house_result[1]}\n")

# Price evolution vs same month previous year

print("4️⃣ Price evolution vs same month of previous year")

query_price_evolution = """
SELECT
    a.date,
    a.med_prix_m2_appartement - b.med_prix_m2_appartement AS evolution_apartment,
    a.med_prix_m2_maison - b.med_prix_m2_maison AS evolution_house
FROM france_monthly a
JOIN france_monthly b
    ON a.date = b.date + INTERVAL '1 year'
ORDER BY a.date DESC
LIMIT 1
"""

evolution = con.execute(query_price_evolution).fetchone()

if evolution:
    print(f"Date: {evolution[0]}")
    print(f"Apartment price evolution €/m²: {evolution[1]}")
    print(f"House price evolution €/m²: {evolution[2]}\n")
else:
    print("Not enough historical data to compute evolution\n")

# Top 10 departments

print("5️⃣ Top 10 departments")

# a) By number of transactions
print("a) By number of transactions")

query_top_transactions = """
SELECT
    libelle_geo,
    (nb_ventes_whole_appartement + nb_ventes_whole_maison) AS total_transactions
FROM top_departments
ORDER BY total_transactions DESC
LIMIT 10
"""

top_tx = con.execute(query_top_transactions).fetchall()

for row in top_tx:
    print(f"{row[0]} → {row[1]} transactions")

print()

# b) By median price per square meter
print("b) By median price per square meter")

query_top_price = """
SELECT
    libelle_geo,
    (med_prix_m2_whole_appartement + med_prix_m2_whole_maison) / 2 AS avg_median_price_m2
FROM top_departments
ORDER BY avg_median_price_m2 DESC
LIMIT 10
"""

top_price = con.execute(query_top_price).fetchall()

for row in top_price:
    print(f"{row[0]} → {round(row[1], 2)} €/m²")

con.close()


