import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

# Use PG_URL env var first (for both local + Docker),
# fallback to your local-forwarded container.
PG_URL = os.getenv(
    "PG_URL",
    "postgresql+psycopg2://postgres:postgres@localhost:15432/dublin_housing",
)

engine = create_engine(PG_URL)

# Base dir = project root (.../dublin-student-housing)
BASE_DIR = Path(__file__).resolve().parents[3]
DATA_DIR = BASE_DIR / "data" / "raw"


def load_csv_to_table(filename: str, table_name: str) -> None:
    csv_path = DATA_DIR / filename
    if not csv_path.exists():
        print(f"⚠️ {filename} not found at {csv_path}, skipping.")
        return

    df = pd.read_csv(csv_path)
    print(f"Loading {table_name}: {len(df)} rows")

    with engine.begin() as conn:
        # drop to avoid schema conflicts during development
        conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}"'))
        df.to_sql(table_name, conn, if_exists="replace", index=False)

    print(f"✅ Loaded {table_name} into Postgres")


def main():
    # 1) Housing listings
    load_csv_to_table("daft_listings.csv", "daft_listings")

    # 2) Food prices
    load_csv_to_table("food_prices_aldi_tesco_fast.csv", "food_prices")

    # 3) Amenities (this is the missing table)
    load_csv_to_table("dublin_amenities.csv", "amenities")


if __name__ == "__main__":
    main()