# orchestration/pipeline.py

from pathlib import Path

import pandas as pd
from dagster import (
    asset,
    Definitions,
    define_asset_job,
    ScheduleDefinition,
)

# Paths inside **containers** (Dagster + Streamlit)
DATA_ROOT = Path("/app/data/raw")

HOUSING_CSV = DATA_ROOT / "daft_listings.csv"
FOOD_CSV = DATA_ROOT / "food_prices_aldi_tesco_fast.csv"
AMENITIES_CSV = DATA_ROOT / "dublin_amenities.csv"


# ---------------- ASSETS ----------------

@asset(description="Raw Daft listings from CSV")
def daft_listings_csv() -> pd.DataFrame:
    return pd.read_csv(HOUSING_CSV)


@asset(description="Raw food prices (Aldi vs Tesco) from CSV")
def food_prices_csv() -> pd.DataFrame:
    return pd.read_csv(FOOD_CSV)


@asset(description="Dublin amenities from CSV")
def amenities_csv() -> pd.DataFrame:
    return pd.read_csv(AMENITIES_CSV)


# ---------------- JOB + SCHEDULE ----------------

daily_refresh_job = define_asset_job(
    name="daily_refresh_job",
    selection="*",
)

daily_refresh_schedule = ScheduleDefinition(
    job=daily_refresh_job,
    cron_schedule="0 3 * * *",  # 03:00 UTC every day
)


# ---------------- DEFINITIONS (THIS is what Dagster needs) ----------------

defs = Definitions(
    assets=[daft_listings_csv, food_prices_csv, amenities_csv],
    schedules=[daily_refresh_schedule],
)