import pandas as pd
from pymongo import MongoClient
from pathlib import Path

MONGO_URI = "mongodb://localhost:27017"
MONGO_DB = "dublin_housing"
BASE = Path("/Users/suley/Desktop/dublin-student-housing/data/raw")

FILES = {
    "daft_listings": BASE / "daft_properties_playwright.csv",
    "food_prices": BASE / "food_prices_aldi_tesco_fast.csv",
    "amenities": BASE / "dublin_amenities.csv",
}


def load_csv_to_mongo(collection_name, csv_path):
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    if not csv_path.exists():
        raise FileNotFoundError(csv_path)

    df = pd.read_csv(csv_path)
    records = df.to_dict(orient="records")

    col = db[collection_name]
    col.delete_many({})
    if records:
        col.insert_many(records)

    client.close()


def main():
    for coll, path in FILES.items():
        load_csv_to_mongo(coll, path)
        print(f"Loaded {path.name} into Mongo collection '{coll}'")


if __name__ == "__main__":
    main()