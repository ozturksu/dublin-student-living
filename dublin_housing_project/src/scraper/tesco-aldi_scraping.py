import re
import time
import random
import urllib.parse

import requests
import pandas as pd
from bs4 import BeautifulSoup


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Safari/605.1.15",
    "Accept-Language": "en-IE,en;q=0.9",
}

SEARCH_TERMS = [
    "milk",
    "bread",
    "eggs",
    "butter",
    "cheese",
    "yogurt",
    "pasta",
    "rice",
    "noodles",
    "beans",
    "lentils",
    "tuna",
    "pasta sauce",
    "tomatoes",
    "oats",
    "cereal",
    "frozen pizza",
    "frozen chips",
    "frozen vegetables",
    "chicken",
    "mince",
    "sausages",
    "ham",
    "jam",
    "peanut butter",
    "ketchup",
    "cooking oil",
    "sugar",
    "salt",
    "flour",
    "coffee",
    "tea",
    "juice",
    "fruit",
    "vegetables",
    "ready meal",
]

FOOD_INCLUDE = [
    "milk", "bread", "wrap", "tortilla", "bagel", "pitta", "naan",
    "egg", "butter", "spread", "margarine",
    "cheese", "yogurt", "yoghurt", "cream",
    "pasta", "spaghetti", "penne", "fusilli", "macaroni",
    "rice", "noodle",
    "bean", "chickpea", "lentil", "tuna", "sardine", "mackerel", "fish", "salmon",
    "sauce", "passata", "tomato", "puree",
    "oat", "porridge", "cereal", "granola", "muesli",
    "pizza", "chips", "fries",
    "vegetable", "veg", "pea", "corn",
    "chicken", "mince", "sausage", "burger", "meatball", "bacon", "ham", "turkey", "salami",
    "jam", "peanut", "hazelnut", "ketchup", "mayonnaise", "mayo", "mustard",
    "oil", "sugar", "salt", "flour",
    "coffee", "tea",
    "juice",
    "apple", "banana", "orange", "grape", "strawber", "raspber", "blueber",
    "onion", "potato", "carrot", "pepper", "cucumber", "lettuce", "salad",
    "lasagne", "cottage pie", "macaroni", "chilli", "curry",
    "soup",
    "yoghurt drink", "protein yoghurt", "protein yogurt", "protein bar",
]

NON_FOOD_EXCLUDE = [
    "bin bag", "bin liner", "foil", "cling film",
    "washing up", "laundry", "detergent", "dishwasher", "cleaner", "bleach",
    "toilet roll", "kitchen roll", "tissue", "nappy", "wipe", "sanitary",
    "shampoo", "conditioner", "shower", "soap", "deodorant", "toothpaste",
    "dog", "cat", "pet", "litter",
    "candle", "battery", "bag", "bbq charcoal",
]

MAX_PAGES_PER_TERM = 3
MAX_TOTAL_ROWS = 2200


def clean_price_to_float(text):
    if not text:
        return None
    txt = text.replace(",", ".")
    m = re.search(r"(\d+\.\d+|\d+)", txt)
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None


def extract_measure_from_text(text):
    if not text:
        return None
    m_multi = re.search(r"(\d+)\s*x\s*(\d+(?:\.\d+)?)\s*(g|kg|ml|l)", text, flags=re.IGNORECASE)
    if m_multi:
        return m_multi.group(0).strip()
    m = re.search(r"(\d+(?:\.\d+)?)\s*(ml|l|ltr|litre|litres|kg|g)\b", text, flags=re.IGNORECASE)
    if m:
        return m.group(0).strip()
    return None


def infer_quantity_and_unit_from_text(text):
    if not text:
        return None, None
    lower = text.lower()
    m_multi = re.search(r"(\d+)\s*x\s*(\d+(?:\.\d+)?)\s*(g|kg|ml|l)", lower)
    if m_multi:
        count = int(m_multi.group(1))
        amount = float(m_multi.group(2))
        unit = m_multi.group(3)
        if unit == "g":
            return count * amount / 1000.0, "kg"
        if unit == "kg":
            return count * amount, "kg"
        if unit == "ml":
            return count * amount / 1000.0, "L"
        if unit == "l":
            return count * amount, "L"
    m_litre = re.search(r"(\d+(?:\.\d+)?)\s*(l|ltr|litre|litres)", lower)
    if m_litre:
        return float(m_litre.group(1)), "L"
    m_ml = re.search(r"(\d+(?:\.\d+)?)\s*ml", lower)
    if m_ml:
        return float(m_ml.group(1)) / 1000.0, "L"
    m_kg = re.search(r"(\d+(?:\.\d+)?)\s*kg", lower)
    if m_kg:
        return float(m_kg.group(1)), "kg"
    m_g = re.search(r"(\d+(?:\.\d+)?)\s*g", lower)
    if m_g:
        return float(m_g.group(1)) / 1000.0, "kg"
    m_eggs = re.search(r"(\d+)\s*(pack|pk|box|eggs?)", lower)
    if m_eggs:
        return int(m_eggs.group(1)), "egg"
    return None, None


def is_food_title(title):
    if not title:
        return False
    t = title.lower()
    if any(bad in t for bad in NON_FOOD_EXCLUDE):
        return False
    return any(k in t for k in FOOD_INCLUDE)


def parse_aldi_products(html):
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    for tile in soup.select("div.product-tile"):
        link_el = tile.select_one("a.product-tile__link")
        if not link_el or not link_el.get("href"):
            continue
        name_el = tile.select_one(".product-tile__name p")
        if not name_el:
            name_el = tile.select_one(".product-tile__title")
        title = name_el.get_text(strip=True) if name_el else None
        if not is_food_title(title):
            continue
        href = link_el["href"]
        product_url = urllib.parse.urljoin("https://www.aldi.ie", href)
        unit_el = tile.select_one(".product-tile__unit-of-measurement p")
        unit_measure = unit_el.get_text(strip=True) if unit_el else None
        price_el = tile.select_one(".product-tile__price .base-price__regular span")
        price_eur = clean_price_to_float(price_el.get_text(strip=True)) if price_el else None
        img_el = tile.select_one("img.base-image")
        image_url = img_el.get("src") if img_el and img_el.get("src") else None
        rows.append(
            {
                "retailer": "ALDI",
                "title": title,
                "price_eur": price_eur,
                "unit_measure": unit_measure,
                "product_url": product_url,
                "image_url": image_url,
            }
        )
    return rows


def parse_tesco_products(html):
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    for li in soup.select("li[data-testid]"):
        link = li.select_one("h2 a, h3 a")
        if not link or not link.get("href"):
            continue
        title = link.get_text(strip=True)
        if not is_food_title(title):
            continue
        product_url = urllib.parse.urljoin("https://www.tesco.ie", link["href"])
        price_block = li.select_one("div.ddsweb-buybox__price")
        price_eur = None
        if price_block:
            main_p = price_block.find("p")
            if main_p:
                price_eur = clean_price_to_float(main_p.get_text(strip=True))
        img_el = li.find("img")
        image_url = img_el.get("src") if img_el and img_el.get("src") else None
        unit_measure = extract_measure_from_text(title)
        rows.append(
            {
                "retailer": "Tesco",
                "title": title,
                "price_eur": price_eur,
                "unit_measure": unit_measure,
                "product_url": product_url,
                "image_url": image_url,
            }
        )
    return rows


def scrape_aldi(session, search_terms):
    all_rows = []
    seen_urls = set()
    for term in search_terms:
        for page in range(1, MAX_PAGES_PER_TERM + 1):
            if len(all_rows) >= MAX_TOTAL_ROWS // 2:
                return all_rows
            if page == 1:
                url = "https://www.aldi.ie/results?q={}".format(urllib.parse.quote_plus(term))
            else:
                url = "https://www.aldi.ie/results?q={}&page={}".format(
                    urllib.parse.quote_plus(term), page
                )
            try:
                resp = session.get(url, headers=HEADERS, timeout=15)
                resp.raise_for_status()
            except Exception:
                break
            rows = parse_aldi_products(resp.text)
            if not rows:
                break
            for r in rows:
                u = r.get("product_url")
                if u and u not in seen_urls:
                    seen_urls.add(u)
                    all_rows.append(r)
            time.sleep(random.uniform(0.25, 0.5))
    return all_rows


def scrape_tesco(session, search_terms):
    all_rows = []
    seen_urls = set()
    for term in search_terms:
        for page in range(1, MAX_PAGES_PER_TERM + 1):
            if len(all_rows) >= MAX_TOTAL_ROWS // 2:
                return all_rows
            url = "https://www.tesco.ie/groceries/en-IE/search?query={}&page={}".format(
                urllib.parse.quote_plus(term), page
            )
            try:
                resp = session.get(url, headers=HEADERS, timeout=15)
                resp.raise_for_status()
            except Exception:
                break
            rows = parse_tesco_products(resp.text)
            if not rows:
                break
            for r in rows:
                u = r.get("product_url")
                key = ("Tesco", u)
                if u and key not in seen_urls:
                    seen_urls.add(key)
                    all_rows.append(r)
            time.sleep(random.uniform(0.25, 0.5))
    return all_rows


def add_canonical_unit_price(df):
    canonical_unit = []
    canonical_unit_price = []
    for _, row in df.iterrows():
        title = str(row.get("title") or "")
        unit_measure = str(row.get("unit_measure") or "")
        price = row.get("price_eur")
        qty, unit = infer_quantity_and_unit_from_text(title + " " + unit_measure)
        if price is None or qty is None or unit is None or qty == 0:
            canonical_unit.append(None)
            canonical_unit_price.append(None)
            continue
        canonical_unit.append(unit)
        canonical_unit_price.append(price / qty)
    df["canonical_unit"] = canonical_unit
    df["canonical_unit_price"] = canonical_unit_price
    return df


def main():
    session = requests.Session()
    aldi_rows = scrape_aldi(session, SEARCH_TERMS)
    tesco_rows = scrape_tesco(session, SEARCH_TERMS)
    all_rows = aldi_rows + tesco_rows
    df = pd.DataFrame(
        all_rows,
        columns=[
            "retailer",
            "title",
            "price_eur",
            "unit_measure",
            "product_url",
            "image_url",
        ],
    )
    df = df.drop_duplicates(subset=["retailer", "product_url"]).reset_index(drop=True)
    if not df.empty:
        df = add_canonical_unit_price(df)
    df.to_csv("/Users/suley/Desktop/dublin-student-housing/data/raw/food_prices_aldi_tesco_fast.csv", index=False)
    print("Total rows:", len(df))


if __name__ == "__main__":
    main()