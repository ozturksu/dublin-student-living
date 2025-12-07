# orchestration/pipeline.py

import os
from pathlib import Path

import pandas as pd
from dagster import (
    asset,
    Definitions,
    define_asset_job,
    ScheduleDefinition,
)

# --- Paths inside the Docker containers ---
# In both the dagster and streamlit images we copy local `data/` -> `/app/data`
DATA_ROOT = Path("/app/data/raw")

HOUSING_CSV = DATA_ROOT / "daft_listings.csv"
FOOD_CSV = DATA_ROOT / "food_prices_aldi_tesco_fast.csv"
AMENITIES_CSV = DATA_ROOT / "dublin_amenities.csv"


# ---------- Assets ----------

@asset(
    name="daft_listings_csv",
    description="Raw Daft listings scraped data from CSV.",
)
def daft_listings_csv() -> pd.DataFrame:
    return pd.read_csv(HOUSING_CSV)


@asset(
    name="food_prices_csv",
    description="Raw food price data (Aldi vs Tesco) from CSV.",
)
def food_prices_csv() -> pd.DataFrame:
    return pd.read_csv(FOOD_CSV)


@asset(
    name="amenities_csv",
    description="Dublin amenities dataset from CSV.",
)
def amenities_csv() -> pd.DataFrame:
    return pd.read_csv(AMENITIES_CSV)


# ---------- Jobs & schedules ----------

# Simple asset job that materializes everything
daily_refresh_job = define_asset_job(
    name="daily_refresh_job",
    selection="*",
)

# Run once per day at 03:00 UTC (adjust if you like)
daily_refresh_schedule = ScheduleDefinition(
    job=daily_refresh_job,
    cron_schedule="0 3 * * *",
)


# ---------- Definitions object (THIS is what Dagster expects) ----------

defs = Definitions(
    assets=[
        daft_listings_csv,
        food_prices_csv,
        amenities_csv,
    ],
    schedules=[daily_refresh_schedule],
)