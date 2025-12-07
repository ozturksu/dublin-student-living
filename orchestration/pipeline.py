import sys
import subprocess
from pathlib import Path

from dagster import job, op

PROJECT_ROOT = Path("/app/dublin_housing_project")
SCRAPER_ROOT = PROJECT_ROOT / "src" / "scraper"
ETL_ROOT = PROJECT_ROOT / "src" / "etl"
DATA_ROOT = Path("/Users/suley/Desktop/dublin-student-housing/data/raw")

def run_script(path: Path):
    print(f"▶ Running {path}")
    subprocess.run([sys.executable, str(path)], check=True)


@op
def run_daft_scraper():
    run_script(SCRAPER_ROOT / "daft_scraping.py")


@op
def run_amenities_builder():
    run_script(SCRAPER_ROOT / "build_gtsf_dataset.py")


@op
def run_food_scraper():
    run_script(SCRAPER_ROOT / "tesco-aldi_scraping.py")


@op
def load_postgres():
    run_script(ETL_ROOT / "load_to_postgres.py")


@job
def run_all_scrapers():
    # Order matters: daft first, then amenities (which uses daft), then food, then load to Postgres
    run_daft_scraper()
    run_amenities_builder()
    run_food_scraper()
    load_postgres()