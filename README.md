# DVF Data Lake & Data Warehouse  
**French Real Estate Market Analysis**

## 📌 Project Overview

This project implements a complete **local Data Engineering pipeline** to analyze the French real estate market using **DVF (Demandes de Valeurs Foncières)** open data.

The objective is to design and build a **Data Lake** and a **Data Warehouse**, then perform **Business Intelligence analysis** using SQL, following professional data architecture best practices.

All constraints of the lab are respected:
- Open-source tools only
- Local execution
- No cloud services
- No external database servers

---

## 🗂️ Data Sources

Two datasets provided by **data.gouv.fr** are used:

### 1. `dvf_stats_monthly.csv`
- Monthly aggregated real estate indicators  
- Time dimension (`year-month`)
- Indicators by property type (houses, apartments, etc.)
- Multiple geographic levels (nation, department, municipality)

### 2. `dvf_stats_aggregated.csv`
- Globally aggregated real estate indicators
- No time dimension
- Suitable for territorial comparisons

Both datasets are ingested **unchanged** into the RAW layer.

## 🔄 Data Pipeline

### Step 1 – RAW (Ingestion)

Original DVF CSV files are stored in the RAW layer without any modification.

**Purpose:**
- Preserve the source of truth
- Ensure traceability and reproducibility
- Enable auditing and reprocessing

---

### Step 2 – STAGING (Data Cleaning)

The STAGING layer prepares the data for analysis.

**Actions performed:**
- Selection of relevant columns
- Standardization of column names and data types
- Handling missing values
- Preparation of geographic dimensions
- Creation of time dimensions for monthly data

**Outputs:**
- `dvf_stats_monthly_clean.csv`
- `dvf_stats_aggregated_clean.csv`

---

### Step 3 – CURATED (BI Preparation)

The CURATED layer contains datasets with clear business meaning.

**BI-ready datasets created:**
1. Monthly real estate indicators for France  
2. Top departments according to real estate indicators  

**Outputs:**
- `dvf_france_monthly_bi.csv`
- `dvf_top_departments_bi.csv`

These datasets are directly usable for SQL analysis or BI tools.

---

### Step 4 – Data Warehouse

A **local Data Warehouse** is created using **DuckDB**.

**Actions:**
- Creation of a DuckDB database file
- Loading curated datasets as SQL tables
- Storage of the database in the `warehouse/` folder

**Tables:**
- `france_monthly`
- `top_departments`

---

### Step 5 – Data Warehouse Validation

Validation checks are performed using SQL queries executed via Python:
- Verification that all tables exist
- Verification that tables contain data
- Verification of row count consistency with curated source files

This step ensures the reliability of the pipeline.

---

### Step 6 – Business Intelligence Analysis

SQL queries are executed on the Data Warehouse to answer key business questions:
- Availability of data for January 2026
- Latest available month
- Median prices per square meter for houses and apartments
- Price evolution compared to the same month of the previous year
- Top 10 departments by transaction volume and by median price

---

### Step 7 – Market Interpretation

Based on the analytical results, a short market interpretation is provided, addressing:
- Market trend (increasing, decreasing, or stable)
- Relationship between price levels and transaction volumes
- Regional disparities across France

---

### Step 8 – Data Quality & Architecture

Final considerations include:
- Importance of preserving RAW data
- Why CURATED data is BI-ready
- Data quality limitations of DVF statistics
- Why DuckDB is an effective alternative to a traditional Data Warehouse for this lab

---

## 🧪 Tools & Technologies

- **Python**
- **DuckDB**
- **CSV files**
- **SQL**

All tools are open-source and run locally.

---

## 🏁 Conclusion

This project demonstrates how a complete end-to-end data pipeline can be built locally using open data and open-source tools, while respecting professional Data Engineering standards.

---

**Author:** Gilles SOUOP  


END OF README
====================================================
