import math
import json
import requests
import pandas as pd
from pathlib import Path


OVERPASS_URL = "https://overpass-api.de/api/interpreter"

DUBLIN_MIN_LAT = 53.20
DUBLIN_MAX_LAT = 53.45
DUBLIN_MIN_LON = -6.50
DUBLIN_MAX_LON = -6.00

AMENITY_QUERIES = [
    'node["amenity"="cafe"]',
    'node["amenity"="fast_food"]',
    'node["amenity"="restaurant"]',
    'node["amenity"="bar"]',
    'node["amenity"="pub"]',
    'node["amenity"="library"]',
    'node["amenity"="college"]',
    'node["amenity"="university"]',
    'node["amenity"="bank"]',
    'node["amenity"="atm"]',
    'node["shop"="supermarket"]',
    'node["shop"="convenience"]',
    'node["shop"="bakery"]',
    'node["leisure"="fitness_centre"]',
    'node["leisure"="sports_centre"]',
]





DATA_ROOT = Path("/Users/suley/Desktop/dublin-student-housing/data/raw")
AMENITIES_CSV = DATA_ROOT / "dublin_amenities.csv"
DAFT_INPUT = DATA_ROOT / "daft_listings.csv"
DAFT_WITH_AMENITIES_OUTPUT = DATA_ROOT / "daft_with_amenities.csv"

def build_overpass_query():
    bbox = f"({DUBLIN_MIN_LAT},{DUBLIN_MIN_LON},{DUBLIN_MAX_LAT},{DUBLIN_MAX_LON})"
    parts = []
    for q in AMENITY_QUERIES:
        parts.append(f"{q}{bbox};")
    body = "[out:json][timeout:60];(" + "".join(parts) + ");out body;"
    return body


def fetch_amenities():
    query = build_overpass_query()
    resp = requests.post(OVERPASS_URL, data={"data": query}, timeout=120)
    resp.raise_for_status()
    data = resp.json()
    rows = []
    for el in data.get("elements", []):
        if el.get("type") != "node":
            continue
        lat = el.get("lat")
        lon = el.get("lon")
        tags = el.get("tags", {})
        name = tags.get("name")
        category = None
        if "amenity" in tags:
            category = tags["amenity"]
        elif "shop" in tags:
            category = tags["shop"]
        elif "leisure" in tags:
            category = tags["leisure"]
        if category is None:
            continue
        rows.append(
            {
                "name": name,
                "category": category,
                "lat": lat,
                "lon": lon,
            }
        )
    df = pd.DataFrame(rows)
    df = df.dropna(subset=["lat", "lon"]).reset_index(drop=True)
    return df


def haversine(lat1, lon1, lat2, lon2):
    r = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r * c


def attach_nearest_amenities(daft_df, amenities_df, max_distance_km=2.0):
    categories = amenities_df["category"].unique()
    amenity_groups = {
        cat: amenities_df[amenities_df["category"] == cat].reset_index(drop=True)
        for cat in categories
    }

    for cat, sub in amenity_groups.items():
        lats = sub["lat"].to_numpy()
        lons = sub["lon"].to_numpy()
        names = sub["name"].fillna("").to_numpy()

        nearest_name_col = []
        nearest_dist_col = []

        for _, row in daft_df.iterrows():
            lat = row["latitude"]
            lon = row["longitude"]
            best_dist = None
            best_name = None
            for s_lat, s_lon, s_name in zip(lats, lons, names):
                d = haversine(lat, lon, s_lat, s_lon)
                if best_dist is None or d < best_dist:
                    best_dist = d
                    best_name = s_name
            if best_dist is not None and best_dist <= max_distance_km:
                nearest_name_col.append(best_name)
                nearest_dist_col.append(best_dist)
            else:
                nearest_name_col.append(None)
                nearest_dist_col.append(None)

        safe_cat = cat.replace(" ", "_")
        daft_df[f"nearest_{safe_cat}_name"] = nearest_name_col
        daft_df[f"nearest_{safe_cat}_dist_km"] = nearest_dist_col

    return daft_df


def main():
    amenities_df = fetch_amenities()
    amenities_df.to_csv("/Users/suley/Desktop/dublin-student-housing/data/raw/dublin_amenities.csv", index=False)
    print("Saved amenities:", len(amenities_df))

    if DAFT_INPUT.exists():
        daft_df = pd.read_csv(DAFT_INPUT)
        if {"latitude", "longitude"}.issubset(daft_df.columns):
            daft_with = attach_nearest_amenities(daft_df, amenities_df)
            daft_with.to_csv(DAFT_WITH_AMENITIES_OUTPUT, index=False)
            print("Saved Daft listings with amenities:", len(daft_with))
        else:
            print("Daft CSV missing latitude/longitude; skipping enrichment.")
    else:
        print("Daft CSV not found; only amenities CSV was created.")


if __name__ == "__main__":
    main()