# Football & Weather Analytics on Snowflake

## 📌 Project Overview

This project is an **end-to-end data engineering pipeline** built to
ingest, model, and analyze football match data using **Snowflake** as
the cloud data warehouse and **Streamlit** for analytics visualization.

The main objective is to **demonstrate hands-on Snowflake data
engineering skills** in a clean, production-oriented way, suitable as a
**first portfolio project using Snowflake**.

The project focuses on: - Raw data ingestion - Layered data modeling
(RAW → STG → MART) - SQL-based transformations - Analytics-ready
outputs - Interactive data consumption

------------------------------------------------------------------------

## 🧠 Use Case

The dataset covers **Primeira Liga 2022/23** football matches.\
From this data, the project computes:

-   League standings
-   Points per game (PPG)
-   Goals scored and conceded
-   Time-series performance per team

The architecture is intentionally designed to be **extensible**, with a
clear path to integrate **weather data (NOAA)** and build match-level
football + weather analytics in future iterations.

------------------------------------------------------------------------

## 🏗️ Architecture

    football-weather-analytics
    │
    ├── ingestion/
    │   ├── ingestAll.py
    │   ├── csvFootball.py
    │   └── snowflake_io.py
    │
    ├── sql/
    │   ├── 01_init.sql
    │   ├── 10_stgMatches.sql
    │   └── 20_martKpis.sql
    │
    ├── dashboard/
    │   ├── app.py
    │   └── data/
    │       └── exportToCSV.py
    │
    ├── config/
    ├── .env
    ├── requirements.txt
    ├── Makefile
    └── README.md

------------------------------------------------------------------------

## 🔄 Data Flow

1.  **Ingestion**\
    Football match data is downloaded from a public CSV source and
    ingested into Snowflake RAW tables using VARIANT.

2.  **Transformation**\
    SQL transformations build STG and MART layers inside Snowflake.

3.  **Consumption**\
    Streamlit dashboard queries Snowflake directly and displays
    analytics.

------------------------------------------------------------------------

## 🗄️ Data Modeling Strategy

### Schemas

  Schema   Purpose
  -------- ---------------------------
  RAW      Immutable ingestion layer
  STG      Typed, cleaned data
  MART     Analytics-ready KPIs

------------------------------------------------------------------------

## 📊 Dashboard Features

-   League standings with sorting and highlights\
-   Points per game (PPG) over time\
-   Monthly goals scored vs conceded\
-   Parameterized season range\
-   Cached Snowflake queries

------------------------------------------------------------------------

## 🧰 Tech Stack

-   Python 3
-   Snowflake
-   SQL
-   pandas
-   requests
-   Streamlit
-   python-dotenv

------------------------------------------------------------------------

## ⚙️ Setup & Usage

``` bash
make setup
make ingest
make dashboard
```

------------------------------------------------------------------------

## 🚀 Next Steps

-   Weather (NOAA) ingestion
-   dbt models
-   Incremental loads
-   Snowflake tasks & streams

------------------------------------------------------------------------

## 👤 Author

**Luís Figueiredo**\
Data Engineer --- 5+ years of experience
