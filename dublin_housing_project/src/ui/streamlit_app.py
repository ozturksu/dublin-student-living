import re
import os

import numpy as np
import pandas as pd

import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

# ------------------------------------------------------------------
# Paths to CSV fallbacks (these are in your repo: data/raw/*.csv)
# ------------------------------------------------------------------
HOUSING_CSV = "data/raw/daft_listings.csv"
FOOD_CSV = "data/raw/food_prices_aldi_tesco_fast.csv"
AMENITIES_CSV = "data/raw/dublin_amenities.csv"

# ------------------------------------------------------------------
# Streamlit page config
# ------------------------------------------------------------------
st.set_page_config(
    page_title="Dublin Student Living Dashboard",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ------------------------------------------------------------------
# Database connection (optional on Streamlit Cloud)
# ------------------------------------------------------------------
# On your laptop / Docker:
#   export PG_URL="postgresql+psycopg2://postgres:postgres@localhost:15432/dublin_housing"
#
# On Streamlit Cloud:
#   Add PG_URL as a secret, pointing to a **remote** Postgres (if you use one).
PG_URL = os.getenv("PG_URL")  # no default for Cloud

if PG_URL:
    engine = create_engine(PG_URL, pool_pre_ping=True)
else:
    engine = None

# ------------------------------------------------------------------
# Mapbox token (optional)
# ------------------------------------------------------------------
try:
    MAPBOX_TOKEN = st.secrets.get("mapbox_token", None)
except Exception:
    MAPBOX_TOKEN = None

if MAPBOX_TOKEN:
    px.set_mapbox_access_token(MAPBOX_TOKEN)
    MAP_STYLE = "mapbox://styles/mapbox/light-v10"
else:
    MAP_STYLE = "carto-positron"  # open tile style (no token needed)


# ------------------------------------------------------------------
# Helper functions
# ------------------------------------------------------------------
def convert_to_monthly(price_text, numeric_price):
    s = str(price_text).lower()
    m = re.search(r"(\d[\d,\.]*)", s)
    base = None
    if m:
        try:
            base = float(m.group(1).replace(",", ""))
        except ValueError:
            base = None
    if base is None and pd.notna(numeric_price):
        base = float(numeric_price)
    if base is None:
        return np.nan
    if "week" in s or "wk" in s:
        return base * 4.33
    if "day" in s:
        return base * 30.0
    if "year" in s or "annum" in s:
        return base / 12.0
    return base


def fig_downloads(fig, label):
    """
    Try to offer PNG/PDF downloads, but fail silently if Kaleido/Chrome
    aren't available (e.g. on Streamlit Cloud).
    """
    try:
        png = fig.to_image(format="png", scale=2)
    except Exception:
        # No Kaleido or Chrome → just show the chart, no buttons
        return

    c1, c2 = st.columns(2)
    with c1:
        st.download_button(
            label=f"Download {label} (PNG)",
            data=png,
            file_name=f"{label}.png",
            mime="image/png",
        )

    try:
        pdf = fig.to_image(format="pdf")
        with c2:
            st.download_button(
                label=f"Download {label} (PDF)",
                data=pdf,
                file_name=f"{label}.pdf",
                mime="application/pdf",
            )
    except Exception:
        pass


def haversine(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2
    return 6371 * 2 * np.arcsin(np.sqrt(a))


def parse_fat_percent(title):
    m = re.search(r"(\d+(\.\d+)?)\s*%?\s*fat", title.lower())
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None


def parse_pack_qty(title, unit_measure):
    txt = f"{title} {unit_measure}".lower()
    m = re.search(r"\b(\d+)\s*(pack|pk|x)\b", txt)
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            pass
    m2 = re.search(r"\b(\d+)\s*eggs?\b", txt)
    if m2:
        try:
            return int(m2.group(1))
        except ValueError:
            pass
    return None


def pick_best_candidate(df, target_qty, pref_fn):
    df = df.dropna(subset=["canonical_unit_price"]).copy()
    if df.empty:
        return None
    df["score_extra"] = df.apply(pref_fn, axis=1)
    df["score_price"] = df["canonical_unit_price"] * target_qty
    df = df.sort_values(["score_extra", "score_price"])
    row = df.iloc[0]
    link = None
    for c in ["product_url", "url", "link", "href"]:
        if c in row.index and pd.notna(row[c]):
            link = row[c]
            break
    return {
        "unit_price": float(row["canonical_unit_price"]),
        "item_price": float(row["canonical_unit_price"]) * target_qty,
        "title": row["title"],
        "unit_measure": row.get("unit_measure", None),
        "link": link,
    }


def match_product_by_unit(df, retailer_pattern, rule):
    sub = df[df["retailer"].str.contains(retailer_pattern, case=False, na=False)].copy()
    if sub.empty:
        return None
    sub = sub[sub["canonical_unit"] == rule["canonical_unit"]].copy()
    if sub.empty:
        return None

    def base_filter(row):
        t = row["title"].lower()
        if rule["name"].startswith("Pasta"):
            shapes = ["spaghetti", "fusilli", "penne", "rigatoni", "macaroni", "tagliatelle"]
            if not any(w in t for w in shapes):
                return False
            if any(b in t for b in rule["exclude"]):
                return False
            return True
        if not all(k in t for k in rule["include"]):
            return False
        if any(b in t for b in rule["exclude"]):
            return False
        return True

    sub = sub[sub.apply(base_filter, axis=1)]
    if sub.empty:
        return None

    def pref(row):
        t = row["title"].lower()
        um = str(row.get("unit_measure", "")).lower()

        if rule["name"].startswith("Milk"):
            fat = parse_fat_percent(t) or 3.5
            score = abs(fat - 3.5)
            if any(w in t for w in ["whole", "full fat"]):
                score -= 0.3
            if any(w in t for w in ["low fat", "skim", "semi skim", "semi-skimmed", "skimmed"]):
                score += 0.6
            if any(
                w in t
                for w in [
                    "chocolate",
                    "strawberry",
                    "banana",
                    "protein",
                    "shake",
                    "biscuit",
                    "biscuits",
                    "cookies",
                    "oats",
                ]
            ):
                score += 2.0
            if "uht" in t or "evaporated" in t:
                score += 0.8
            return score

        if rule["name"].startswith("Eggs"):
            qty = parse_pack_qty(t, um) or 6
            return abs(qty - 6)

        if rule["name"].startswith("Bread"):
            score = 0.0
            if "800g" not in t and "800 g" not in t:
                score += 0.5
            if "sliced" not in t:
                score += 0.3
            if any(w in t for w in ["wrap", "roll", "bagel", "baguette", "thins", "pitta", "ciabatta", "bun"]):
                score += 1.5
            return score

        if rule["name"].startswith("Cheddar"):
            score = 0.0
            if "cheddar" not in t or "cheese" not in t:
                score += 2.0
            if any(w in t for w in ["smoked", "applewood"]):
                score += 3.0
            if any(w in t for w in ["spread", "slices", "slice", "grated", "shredded"]):
                score += 1.0
            return score

        if rule["name"].startswith("Pasta"):
            score = 0.0
            if any(
                w in t
                for w in [
                    "sauce",
                    "bake",
                    "ready meal",
                    "meal",
                    "salad",
                    "pot",
                    "soup",
                    "instant",
                    "microwave",
                ]
            ):
                score += 3.0
            if any(w in t for w in ["spaghetti", "fusilli", "penne", "rigatoni", "macaroni", "tagliatelle"]):
                score -= 0.5
            if any(w in t for w in ["ravioli", "tortellini", "lasagne", "lasagna"]):
                score += 1.5
            return score

        if rule["name"].startswith("Rice"):
            score = 0.0
            if any(w in t for w in ["pudding", "cake", "cracker", "crispy", "krispies"]):
                score += 3.0
            return score

        return 0.0

    return pick_best_candidate(sub, rule["target_qty"], pref)


def build_basket(df):
    rules = [
        {
            "name": "Bread (800g loaf)",
            "canonical_unit": "kg",
            "target_qty": 0.8,
            "include": ["bread"],
            "exclude": ["wrap", "roll", "bagel", "baguette", "thins", "pitta", "ciabatta", "bun"],
        },
        {
            "name": "Cheddar cheese block (200g)",
            "canonical_unit": "kg",
            "target_qty": 0.2,
            "include": ["cheddar", "cheese"],
            "exclude": ["smoked", "applewood", "spread", "slices", "slice", "grated", "shredded"],
        },
        {
            "name": "Eggs (6)",
            "canonical_unit": "egg",
            "target_qty": 6.0,
            "include": ["egg"],
            "exclude": ["waffle", "pancake", "omelette"],
        },
        {
            "name": "Milk (1L, whole)",
            "canonical_unit": "L",
            "target_qty": 1.0,
            "include": ["milk"],
            "exclude": [
                "biscuit",
                "biscuits",
                "cookie",
                "oats",
                "cereal",
                "yogurt",
                "yoghurt",
                "dessert",
                "shake",
            ],
        },
        {
            "name": "Pasta (1kg dry)",
            "canonical_unit": "kg",
            "target_qty": 1.0,
            "include": [],
            "exclude": [
                "sauce",
                "bake",
                "ready meal",
                "meal",
                "salad",
                "pot",
                "soup",
                "instant",
                "microwave",
            ],
        },
        {
            "name": "Rice (1kg)",
            "canonical_unit": "kg",
            "target_qty": 1.0,
            "include": ["rice"],
            "exclude": ["pudding", "cake", "cracker", "crispy", "krispies"],
        },
    ]

    rows = []
    for rule in rules:
        aldi = match_product_by_unit(df, r"^ALDI$", rule)
        tesco = match_product_by_unit(df, r"Tesco", rule)
        if aldi is None or tesco is None:
            continue
        rows.append(
            {
                "item": rule["name"],
                "Aldi price (€)": aldi["item_price"],
                "Tesco price (€)": tesco["item_price"],
                "Saving (Tesco – Aldi)": tesco["item_price"] - aldi["item_price"],
                "Aldi €/unit": aldi["unit_price"],
                "Tesco €/unit": tesco["unit_price"],
                "aldi_unit_measure": aldi["unit_measure"],
                "tesco_unit_measure": tesco["unit_measure"],
                "aldi_title": aldi["title"],
                "tesco_title": tesco["title"],
                "aldi_link": aldi["link"],
                "tesco_link": tesco["link"],
            }
        )

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(rows).sort_values("item").reset_index(drop=True)


# ------------------------------------------------------------------
# LOAD DATA (DB first, CSV fallback)
# ------------------------------------------------------------------
@st.cache_data
def load_data():
    colleges = {
        "Trinity College Dublin": {"lat": 53.3438, "lon": -6.2546},
        "UCD (Belfield)": {"lat": 53.3069, "lon": -6.2237},
        "DCU (Glasnevin)": {"lat": 53.3855, "lon": -6.2568},
        "TU Dublin (Grangegorman)": {"lat": 53.3547, "lon": -6.2787},
        "NCI (IFSC)": {"lat": 53.3486, "lon": -6.2428},
    }

    df_housing = None
    df_food_raw = None
    df_amenities = None

    # 1) Try Postgres if PG_URL is set
    if engine is not None:
        try:
            df_housing = pd.read_sql("select * from daft_listings", engine)
            df_food_raw = pd.read_sql("select * from food_prices", engine)
            df_amenities = pd.read_sql("select * from amenities", engine)
        except SQLAlchemyError as e:
            print(f"DB connection failed, falling back to CSVs: {e}")
            df_housing = df_food_raw = df_amenities = None

    # 2) Fallback to CSVs (e.g. Streamlit Cloud)
    if df_housing is None:
        df_housing = pd.read_csv(HOUSING_CSV)

    df_housing["price_per_month"] = df_housing.apply(
        lambda r: convert_to_monthly(r.get("Price", ""), r.get("PriceNumeric", np.nan)),
        axis=1,
    )
    rename_map = {
        "Latitude": "lat",
        "Longitude": "lon",
        "PropertyType": "type",
        "BedroomsNumeric": "bedrooms",
        "AreaName": "area",
        "URL": "link",
    }
    df_housing = df_housing.rename(
        columns={k: v for k, v in rename_map.items() if k in df_housing.columns}
    )
    df_housing = df_housing.dropna(subset=["lat", "lon", "price_per_month"])
    df_housing["bedrooms"] = pd.to_numeric(df_housing.get("bedrooms", 0), errors="coerce").fillna(0)
    df_housing["type"] = df_housing.get("type", "Unknown").fillna("Unknown")

    # Food
    if df_food_raw is None:
        df_food_raw = pd.read_csv(FOOD_CSV)

    df_food_raw["retailer"] = df_food_raw["retailer"].astype(str).str.strip()
    df_food_raw["price_eur"] = pd.to_numeric(df_food_raw["price_eur"], errors="coerce")
    df_food_raw["canonical_unit_price"] = pd.to_numeric(
        df_food_raw["canonical_unit_price"], errors="coerce"
    )
    df_food_raw["title"] = df_food_raw["title"].astype(str)
    df_food = build_basket(df_food_raw)

    # Amenities
    if df_amenities is None:
        df_amenities = pd.read_csv(AMENITIES_CSV)

    if "category" in df_amenities.columns and "type" not in df_amenities.columns:
        df_amenities = df_amenities.rename(columns={"category": "type"})
    df_amenities["lat"] = pd.to_numeric(df_amenities["lat"], errors="coerce")
    df_amenities["lon"] = pd.to_numeric(df_amenities["lon"], errors="coerce")
    df_amenities = df_amenities.dropna(subset=["lat", "lon"])

    return colleges, df_housing, df_food, df_amenities


# ------------------------------------------------------------------
# MAIN APP
# ------------------------------------------------------------------
colleges, df_housing, df_food, df_amenities = load_data()

st.sidebar.header("🎓 Student preferences")
selected_college = st.sidebar.selectbox("Select your college", list(colleges.keys()))
college_lat = colleges[selected_college]["lat"]
college_lon = colleges[selected_college]["lon"]

st.sidebar.subheader("🏠 Housing filters")

df_housing["dist_km"] = df_housing.apply(
    lambda r: haversine(r["lat"], r["lon"], college_lat, college_lon),
    axis=1,
)

min_price = int(df_housing["price_per_month"].min())
max_price = int(df_housing["price_per_month"].max())

max_rent = st.sidebar.slider("Max monthly rent (€)", min_price, min(max_price, 4000), 2000, step=50)
max_dist = st.sidebar.slider("Max distance to campus (km)", 1, 30, 10)

types_available = sorted(df_housing["type"].unique())
selected_types = st.sidebar.multiselect(
    "Property types",
    options=types_available,
    default=types_available,
)

min_beds = st.sidebar.number_input("Min bedrooms", 0, 6, 1)

filtered_housing = df_housing[
    (df_housing["price_per_month"] <= max_rent)
    & (df_housing["dist_km"] <= max_dist)
    & (df_housing["type"].isin(selected_types))
    & (df_housing["bedrooms"] >= min_beds)
].copy()

st.title(f"Dublin Student Living – {selected_college}")

col1, col2, col3, col4 = st.columns(4)
col1.metric("Matching properties", len(filtered_housing))
col2.metric(
    "Avg rent (filtered)",
    f"€{filtered_housing['price_per_month'].mean():.0f}" if not filtered_housing.empty else "N/A",
)
col3.metric(
    "Lowest rent",
    f"€{filtered_housing['price_per_month'].min():.0f}" if not filtered_housing.empty else "N/A",
)
if not df_food.empty:
    weekly_aldi = df_food["Aldi price (€)"].sum()
    col4.metric("Aldi weekly basket", f"€{weekly_aldi:.2f}")
else:
    col4.metric("Aldi weekly basket", "N/A")

tab1, tab2, tab3 = st.tabs(["🗺 Map search", "📊 Rent analysis", "🛒 Living costs & neighbourhood"])

# --- Tab 1: Map ---
with tab1:
    st.subheader(f"Properties within {max_dist} km of {selected_college}")
    if not filtered_housing.empty:
        fig_map = px.scatter_mapbox(
            filtered_housing,
            lat="lat",
            lon="lon",
            color="price_per_month",
            size="price_per_month",
            color_continuous_scale="Turbo",
            mapbox_style=MAP_STYLE,
            zoom=11,
            hover_name="area",
            hover_data={"price_per_month": True, "link": True, "dist_km": True},
            height=550,
        )
        fig_map.update_layout(template="plotly_white")
        fig_downloads(fig_map, "Rent_Map")
        st.plotly_chart(fig_map, use_container_width=True)

        st.markdown("### Property details")
        cols = ["area", "type", "bedrooms", "price_per_month", "dist_km", "link"]
        cols = [c for c in cols if c in filtered_housing.columns]
        st.dataframe(
            filtered_housing[cols].sort_values("price_per_month"),
            column_config={
                "price_per_month": st.column_config.NumberColumn("Rent (€)", format="€%d"),
                "dist_km": st.column_config.NumberColumn("Distance (km)", format="%.2f km"),
                "link": st.column_config.LinkColumn("Daft listing"),
            },
            use_container_width=True,
        )
    else:
        st.info("No properties match your filters.")

# --- Tab 2: Rent analysis ---
with tab2:
    if not filtered_housing.empty:
        col_a, col_b = st.columns(2)

        with col_a:
            st.subheader("Rent distribution (filtered)")
            fig_hist = px.histogram(
                filtered_housing,
                x="price_per_month",
                nbins=20,
                color="type",
                labels={"price_per_month": "Monthly rent (€)"},
                color_discrete_sequence=px.colors.qualitative.Set2,
            )
            fig_hist.update_layout(template="plotly_white")
            fig_downloads(fig_hist, "Rent_Distribution")
            st.plotly_chart(fig_hist, use_container_width=True)

        with col_b:
            st.subheader("Average rent by area")
            avg_area = (
                filtered_housing.groupby("area")["price_per_month"]
                .mean()
                .reset_index()
                .sort_values("price_per_month")
            )
            fig_area = px.bar(
                avg_area,
                x="area",
                y="price_per_month",
                color="price_per_month",
                labels={"price_per_month": "Avg rent (€)"},
                color_continuous_scale="Viridis",
            )
            fig_area.update_layout(xaxis_tickangle=-45, template="plotly_white")
            fig_downloads(fig_area, "Average_Rent_by_Area")
            st.plotly_chart(fig_area, use_container_width=True)

        st.subheader("Rent vs distance from campus")
        fig_scatter = px.scatter(
            filtered_housing,
            x="dist_km",
            y="price_per_month",
            color="type",
            labels={"dist_km": "Distance to campus (km)", "price_per_month": "Monthly rent (€)"},
            color_discrete_sequence=px.colors.qualitative.Set2,
        )
        fig_scatter.update_layout(template="plotly_white")
        fig_downloads(fig_scatter, "Rent_vs_Distance")
        st.plotly_chart(fig_scatter, use_container_width=True)
    else:
        st.info("No housing data to analyse with current filters.")

# --- Tab 3: Costs & amenities ---
with tab3:
    st.header("Weekly living costs")
    st.markdown(
        "Basket items are built from canonical unit prices (1L milk, 1kg rice, 6 eggs, etc.) "
        "and filtered so we compare like-for-like products only for bread, cheese, eggs, milk, pasta and rice."
    )

    if not df_food.empty:
        total_aldi = df_food["Aldi price (€)"].sum()
        total_tesco = df_food["Tesco price (€)"].sum()
        weekly_diff = total_tesco - total_aldi
        monthly_diff = weekly_diff * 4.33

        c1, c2, c3 = st.columns(3)
        c1.metric("Aldi weekly basket", f"€{total_aldi:.2f}")
        c2.metric("Tesco weekly basket", f"€{total_tesco:.2f}")
        c3.metric("Weekly saving (Aldi vs Tesco)", f"€{weekly_diff:.2f}")
        st.markdown(f"Approx monthly saving with this basket: **€{monthly_diff:.0f}**")

        st.subheader("Item-level prices, products and savings")
        st.dataframe(
            df_food[
                [
                    "item",
                    "Aldi price (€)",
                    "Tesco price (€)",
                    "Saving (Tesco – Aldi)",
                    "Aldi €/unit",
                    "Tesco €/unit",
                    "aldi_unit_measure",
                    "tesco_unit_measure",
                    "aldi_title",
                    "tesco_title",
                    "aldi_link",
                    "tesco_link",
                ]
            ],
            column_config={
                "Aldi price (€)": st.column_config.NumberColumn("Aldi price (€)", format="€%.2f"),
                "Tesco price (€)": st.column_config.NumberColumn("Tesco price (€)", format="€%.2f"),
                "Saving (Tesco – Aldi)": st.column_config.NumberColumn(
                    "Saving (Tesco – Aldi)", format="€%.2f"
                ),
                "Aldi €/unit": st.column_config.NumberColumn("Aldi €/unit", format="€%.2f"),
                "Tesco €/unit": st.column_config.NumberColumn("Tesco €/unit", format="€%.2f"),
                "aldi_link": st.column_config.LinkColumn("Aldi link"),
                "tesco_link": st.column_config.LinkColumn("Tesco link"),
            },
            use_container_width=True,
        )

        df_plot = df_food.melt(
            id_vars=["item"], value_vars=["Aldi price (€)", "Tesco price (€)"], var_name="Store", value_name="Price"
        )
        fig_food = px.bar(
            df_plot,
            x="item",
            y="Price",
            color="Store",
            barmode="group",
            labels={"Price": "Price (€)"},
            title="Basket item prices by store",
            color_discrete_sequence=px.colors.qualitative.Set2,
        )
        fig_food.update_layout(xaxis_tickangle=-30, template="plotly_white")
        fig_downloads(fig_food, "Basket_Prices")
        st.plotly_chart(fig_food, use_container_width=True)
    else:
        st.info("Food price data is not available.")

    st.markdown("---")
    st.header("Neighbourhood amenities")

    if not df_amenities.empty:
        df_amenities["dist_to_college"] = df_amenities.apply(
            lambda r: haversine(r["lat"], r["lon"], college_lat, college_lon),
            axis=1,
        )
        radius = st.slider("Amenity search radius around campus (km)", 1, 10, 5)
        nearby = df_amenities[df_amenities["dist_to_college"] <= radius].copy()

        if not nearby.empty:
            amenity_types = sorted(nearby["type"].dropna().unique())
            selected_amenities = st.multiselect(
                "Filter amenity types",
                options=amenity_types,
                default=amenity_types,
            )
            if selected_amenities:
                nearby = nearby[nearby["type"].isin(selected_amenities)]

            a1, a2, a3 = st.columns(3)
            a1.metric("Total amenities", len(nearby))
            a2.metric("Amenity types", nearby["type"].nunique())
            a3.metric("Median distance", f"{nearby['dist_to_college'].median():.2f} km")

            counts = (
                nearby.groupby("type")["name"]
                .count()
                .reset_index()
                .rename(columns={"name": "count"})
                .sort_values("count", ascending=False)
            )
            fig_counts = px.bar(
                counts,
                x="type",
                y="count",
                title=f"Amenity mix within {radius} km of campus",
                color="count",
                color_continuous_scale="Blues",
            )
            fig_counts.update_layout(xaxis_tickangle=-45, template="plotly_white")
            fig_downloads(fig_counts, "Amenity_Counts")
            st.plotly_chart(fig_counts, use_container_width=True)

            nearby["maps_url"] = nearby.apply(
                lambda r: f"https://www.google.com/maps/search/?api=1&query={r['lat']},{r['lon']}",
                axis=1,
            )

            fig_am = px.scatter_mapbox(
                nearby,
                lat="lat",
                lon="lon",
                color="type",
                hover_name="name",
                hover_data={"maps_url": True, "dist_to_college": True},
                mapbox_style=MAP_STYLE,
                zoom=12,
                height=450,
                color_discrete_sequence=px.colors.qualitative.Set2,
            )
            fig_am.update_layout(template="plotly_white")
            fig_downloads(fig_am, "Amenities_Map")
            st.plotly_chart(fig_am, use_container_width=True)
        else:
            st.info(f"No amenities found within {radius} km of {selected_college}.")
    else:
        st.info("Amenities data is not available.")

st.markdown("---")
st.markdown(
    "<div style='text-align: center; color: grey;'>"
    "<small>Data source: PostgreSQL database 'dublin_housing' "
    "(tables: daft_listings, food_prices, amenities) or CSV fallbacks.</small>"
    "</div>",
    unsafe_allow_html=True,
)
