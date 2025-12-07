

import os
from pathlib import Path

import pandas as pd
from dagster import Definitions, ScheduleDefinition, get_dagster_logger, job, op
from pymongo import MongoClient
from sqlalchemy import create_engine

# ---------------------------------------------------------------------
# Paths & connection strings
# These are designed to work INSIDE the Docker containers.
# ---------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parents[1]  # /app
DATA_DIR = BASE_DIR / "data" / "raw"

CSV_DAFT = DATA_DIR / "daft_listings.csv"
CSV_FOOD = DATA_DIR / "food_prices_aldi_tesco_fast.csv"
CSV_AMENITIES = DATA_DIR / "dublin_amenities.csv"

# Inside Docker network, use service names as hosts
PG_URL = os.getenv(
    "PG_URL",
    "postgresql+psycopg2://postgres:postgres@postgres:5432/dublin_housing",
)

MONGO_URL = os.getenv(
    "MONGO_URL",
    "mongodb://mongo:27017",
)


# ---------------------------------------------------------------------
# Extract ops (read CSV files)
# ---------------------------------------------------------------------


@op
def extract_daft():
    logger = get_dagster_logger()
    if not CSV_DAFT.exists():
        logger.warning(f"Daft CSV not found at {CSV_DAFT}")
        return pd.DataFrame()

    df = pd.read_csv(CSV_DAFT)
    logger.info(f"Loaded {len(df)} rows from daft_listings.csv")
    return df


@op
def extract_food():
    logger = get_dagster_logger()
    if not CSV_FOOD.exists():
        logger.warning(f"Food CSV not found at {CSV_FOOD}")
        return pd.DataFrame()

    df = pd.read_csv(CSV_FOOD)
    logger.info(f"Loaded {len(df)} rows from food_prices CSV")
    return df


@op
def extract_amenities():
    logger = get_dagster_logger()
    if not CSV_AMENITIES.exists():
        logger.warning(f"Amenities CSV not found at {CSV_AMENITIES}")
        return pd.DataFrame()

    df = pd.read_csv(CSV_AMENITIES)
    logger.info(f"Loaded {len(df)} rows from dublin_amenities.csv")
    return df


# ---------------------------------------------------------------------
# Load ops – to Postgres & Mongo
# ---------------------------------------------------------------------


@op
def load_postgres(daft: pd.DataFrame, food: pd.DataFrame, amenities: pd.DataFrame):
    """
    Load CSV data into Postgres tables:
      - daft_listings
      - food_prices
      - amenities
    """
    logger = get_dagster_logger()
    engine = create_engine(PG_URL, future=True)

    with engine.begin() as conn:
        if not daft.empty:
            daft.to_sql("daft_listings", conn, if_exists="replace", index=False)
            logger.info(f"Wrote {len(daft)} rows into daft_listings")

        if not food.empty:
            food.to_sql("food_prices", conn, if_exists="replace", index=False)
            logger.info(f"Wrote {len(food)} rows into food_prices")

        if not amenities.empty:
            amenities.to_sql("amenities", conn, if_exists="replace", index=False)
            logger.info(f"Wrote {len(amenities)} rows into amenities")

    logger.info("Postgres load completed.")


@op
def load_mongo(daft: pd.DataFrame, food: pd.DataFrame, amenities: pd.DataFrame):
    """
    Load CSV data into MongoDB collections:
      - daft_listings
      - food_prices
      - amenities
    """
    logger = get_dagster_logger()
    client = MongoClient(MONGO_URL)
    db = client["dublin_housing"]

    if not daft.empty:
        db.daft_listings.delete_many({})
        db.daft_listings.insert_many(daft.to_dict(orient="records"))
        logger.info(f"Inserted {len(daft)} docs into mongo.daft_listings")

    if not food.empty:
        db.food_prices.delete_many({})
        db.food_prices.insert_many(food.to_dict(orient="records"))
        logger.info(f"Inserted {len(food)} docs into mongo.food_prices")

    if not amenities.empty:
        db.amenities.delete_many({})
        db.amenities.insert_many(amenities.to_dict(orient="records"))
        logger.info(f"Inserted {len(amenities)} docs into mongo.amenities")

    logger.info("Mongo load completed.")


# ---------------------------------------------------------------------
# Job – wire everything together
# ---------------------------------------------------------------------


@job
def run_all_ingestion():
    daft = extract_daft()
    food = extract_food()
    amenities = extract_amenities()

    load_postgres(daft, food, amenities)
    load_mongo(daft, food, amenities)


# ---------------------------------------------------------------------
# Schedule & Definitions for Dagster
# ---------------------------------------------------------------------


weekly_ingestion_schedule = ScheduleDefinition(
    job=run_all_ingestion,
    # Every Monday at 05:00 (server time)
    cron_schedule="0 5 * * 1",
)


defs = Definitions(
    jobs=[run_all_ingestion],
    schedules=[weekly_ingestion_schedule],
)