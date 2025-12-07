# Dublin Student Living Dashboard 🏠🎓

An end-to-end data pipeline and dashboard to help students find **affordable housing and living options in Dublin**, centred around major colleges.

The system:

- Scrapes rental and price data into **MongoDB**
- Moves curated data into **PostgreSQL** via **Dagster** jobs
- Serves an interactive **Streamlit** web app with maps, rent analysis, and weekly food basket comparisons

---

## 🌍 What the app does

### 1. Housing search & map
- Interactive map of Dublin rentals
- Filter by:
  - College (Trinity, UCD, DCU, TU Dublin, NCI)
  - Max monthly rent
  - Max distance to campus (km)
  - Property type
  - Minimum bedrooms
- See:
  - Rent vs distance
  - Average rent by area
  - Property table with **clickable Daft.ie links**

### 2. Weekly living costs (food basket)
- Canonical basket: bread, cheddar, eggs, milk, pasta, rice
- Compares **Aldi vs Tesco** using normalised €/unit prices
- Shows:
  - Total weekly basket cost
  - Weekly & monthly savings
  - Item-level table with **clickable product links**

### 3. Neighbourhood amenities
- Amenities around the selected campus (e.g. gyms, supermarkets, cafés)
- Filter by type and radius (km)
- See:
  - Counts by amenity type
  - Map of nearby amenities
  - Direct **Google Maps links** for each location

---

## 🧱 Tech stack

- **Frontend / UI**: Streamlit + Plotly
- **Data**:
  - PostgreSQL (`daft_listings`, `food_prices`, `amenities`)
  - MongoDB (raw scraped data)
  - CSV fallbacks in `data/raw/`
- **Orchestration**: Dagster (Python)
- **Containerisation**: Docker + docker-compose
- **Hosting**: Ubuntu server (e.g. DigitalOcean droplet)

---

## 📁 Project structure (simplified)

```text
dublin-student-living/
├── data/
│   └── raw/
│       ├── daft_listings.csv
│       ├── dublin_amenities.csv
│       └── food_prices_aldi_tesco_fast.csv
├── docker/
│   ├── docker-compose.yml
│   ├── Dockerfile.streamlit
│   └── Dockerfile.dagster
├── dublin_housing_project/
│   └── src/
│       └── ui/
│           └── streamlit_app.py
├── orchestration/
│   ├── __init__.py
│   └── pipeline.py
└── requirements.txt
