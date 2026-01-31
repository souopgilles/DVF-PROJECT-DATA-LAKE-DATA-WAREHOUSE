import pandas as pd
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent

staging = BASE / "data_lake/staging"
curated = BASE / "data_lake/curated"
curated.mkdir(exist_ok=True)

# FRANCE MONTHLY BI DATASET

monthly = pd.read_csv(staging / "dvf_stats_monthly_clean.csv")

# France = niveau national
france_monthly = monthly[
    monthly["echelle_geo"] == "nation"
].copy()

# Sélection BI (clair et lisible)
france_monthly_bi = france_monthly[
    [
        "date",
        "year",
        "month",

        "nb_ventes_maison",
        "med_prix_m2_maison",
        "moy_prix_m2_maison",

        "nb_ventes_appartement",
        "med_prix_m2_appartement",
        "moy_prix_m2_appartement",
    ]
].sort_values("date")

france_monthly_bi.to_csv(
    curated / "dvf_france_monthly_bi.csv",
    index=False
)

print("CURATED 1/2 -> France monthly BI OK ")

# TOP DEPARTMENTS BI DATASET

aggregated = pd.read_csv(staging / "dvf_stats_aggregated_clean.csv")

# Départements uniquement
departments = aggregated[
    aggregated["echelle_geo"] == "departement"
].copy()

# Dataset BI lisible (classements possibles)
top_departments_bi = departments[
    [
        "code_geo",
        "libelle_geo",

        "nb_ventes_whole_appartement",
        "med_prix_m2_whole_appartement",

        "nb_ventes_whole_maison",
        "med_prix_m2_whole_maison",
    ]
]

top_departments_bi.to_csv(
    curated / "dvf_top_departments_bi.csv",
    index=False
)

print("CURATED 2/2 -> Top departments BI OK")
