import os
from pathlib import Path

import pandas as pd
from dagster import (
    Definitions,
    ScheduleDefinition,
    asset,
    define_asset_job,
)
from sqlalchemy import create_engine

# Paths in the container (copied in Dockerfile.dagster)
DATA_DIR = Path("/app/data/raw")
HOUSING_CSV = DATA_DIR / "daft_listings.csv"
FOOD_CSV = DATA_DIR / "food_prices_aldi_tesco_fast.csv"
AMENITIES_CSV = DATA_DIR / "dublin_amenities.csv"

PG_URL = os.getenv(
    "PG_URL",
    "postgresql+psycopg2://postgres:postgres@postgres:5432/dublin_housing",
)


def get_engine():
    return create_engine(PG_URL, pool_pre_ping=True)


@asset
def sync_csv_to_postgres(context):
    """Load the three CSVs into Postgres (replace tables)."""
    engine = get_engine()

    tables = [
        ("daft_listings", HOUSING_CSV),
        ("food_prices", FOOD_CSV),
        ("amenities", AMENITIES_CSV),
    ]

    for table_name, path in tables:
        if not path.exists():
            context.log.warning(f"CSV not found for {table_name}: {path}")
            continue

        context.log.info(f"Loading {table_name} from {path}")
        df = pd.read_csv(path)

        df.to_sql(
            table_name,
            engine,
            if_exists="replace",
            index=False,
        )
        context.log.info(f"Wrote {len(df)} rows to {table_name}")

    return "ok"


# A job that just runs that asset
sync_job = define_asset_job("sync_csv_job", selection=[sync_csv_to_postgres])


# Run every night at 03:00 server time
sync_schedule = ScheduleDefinition(
    name="nightly_sync",
    job=sync_job,
    cron_schedule="0 3 * * *",
)


defs = Definitions(
    assets=[sync_csv_to_postgres],
    schedules=[sync_schedule],
)