import pandas as pd
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent

raw = BASE / "data_lake/raw"
staging = BASE / "data_lake/staging"
staging.mkdir(exist_ok=True)

# MONTHLY DATASET

monthly = pd.read_csv(raw / "dvf_stats_monthly.csv")

# --- Standardisation des noms de colonnes
monthly.columns = (
    monthly.columns
    .str.lower()
    .str.strip()
)

# --- Dimension temps
monthly["year"] = monthly["annee_mois"].astype(str).str[:4].astype(int)
monthly["month"] = monthly["annee_mois"].astype(str).str[5:7].astype(int)
monthly["date"] = pd.to_datetime(monthly["annee_mois"], format="%Y-%m")

# --- Sélection des colonnes utiles
monthly_clean = monthly[[
    "annee_mois",
    "year",
    "month",
    "date",
    "code_geo",
    "libelle_geo",
    "echelle_geo",
    "nb_ventes_maison",
    "med_prix_m2_maison",
    "moy_prix_m2_maison",
    "nb_ventes_appartement",
    "med_prix_m2_appartement",
    "moy_prix_m2_appartement",
]]

# --- Gestion des valeurs manquantes
monthly_clean = monthly_clean.dropna(
    subset=[
        "annee_mois",
        "echelle_geo"
    ]
)

monthly_clean.to_csv(
    staging / "dvf_stats_monthly_clean.csv",
    index=False
)

print("MONTHLY STAGING OK")

# AGGREGATED DATASET

aggregated = pd.read_csv(raw / "dvf_stats_aggregated.csv")

# Standardisation noms
aggregated.columns = aggregated.columns.str.lower().str.strip()

# Sélection colonnes utiles (WHOLE = agrégation globale)
aggregated_clean = aggregated[
    [
        "code_geo",
        "libelle_geo",
        "echelle_geo",

        "nb_ventes_whole_appartement",
        "med_prix_m2_whole_appartement",
        "moy_prix_m2_whole_appartement",

        "nb_ventes_whole_maison",
        "med_prix_m2_whole_maison",
        "moy_prix_m2_whole_maison",

        "nb_ventes_whole_apt_maison",
        "med_prix_m2_whole_apt_maison",
        "moy_prix_m2_whole_apt_maison",
    ]
]

# Suppression lignes sans niveau géographique
aggregated_clean = aggregated_clean.dropna(subset=["echelle_geo"])

aggregated_clean.to_csv(
    staging / "dvf_stats_aggregated_clean.csv",
    index=False
)

print("AGGREGATED STAGING OK")