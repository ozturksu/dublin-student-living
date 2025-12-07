import argparse
import asyncio
import os
import random
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import json
try:
    import pandas as pd  
except ModuleNotFoundError:  
    pd = None  

from playwright.async_api import (
    async_playwright,
    TimeoutError as PlaywrightTimeoutError,
    Error as PlaywrightError,
)
from bs4 import BeautifulSoup  

BASE_URL = "https://www.daft.ie/property-for-rent/dublin"
DEFAULT_MAX_PAGES = 50
PROPERTY_CARD_SELECTORS = [
    '[data-testid="property-card"]',
    '[data-testid="result-card"]',
    'article[data-testid="property-card"]',
]
COOKIE_BUTTON_SELECTORS = [
    'button:has-text("Accept All")',
    'button:has-text("Accept all")',
    'button:has-text("Allow All")',
    'button[data-testid="accept-cookies-button"]',
    '[data-testid="gdpr-compliance-banner-accept-button"]',
]
BLOCK_TEXT_SNIPPETS = [
    "oops something went wrong",
    "service is currently unavailable",
    "please try again soon",
    "verify you are a human",
]


async def scrape_page(page, url: str, page_num: int) -> Tuple[List[Dict[str, Optional[str]]], Optional[int]]:
    """Navigate to a search results page and extract listing data."""
    if page_num == 1 and page.url.startswith(BASE_URL):
        pass
    else:
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=45000)
        except PlaywrightTimeoutError:
            print(f" Navigation timeout for {url}; continuing with current content.")
        await accept_cookie_banner(page)
        try:
            await page.wait_for_load_state("networkidle", timeout=20000)
        except PlaywrightTimeoutError:
            print(" Network idle timeout after navigation; proceeding with DOM snapshot.")

    html = await page.content()
    listings, total_pages = parse_listing_summaries_from_html(html)
    if listings:
        return listings, total_pages

    await _wait_for_results(page)
    await page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
    await page.wait_for_timeout(3000)

    cards = await _query_property_cards(page)
    page_data: List[Dict[str, Optional[str]]] = []

    for listing in cards:
        title = await _safe_inner_text(listing, '[data-testid="title"]')
        price = await _safe_inner_text(listing, '[data-testid="price"]')
        location = await _safe_inner_text(listing, '[data-testid="address"]')
        bedrooms = await _safe_inner_text(listing, '[data-testid="beds"]')
        bathrooms = await _safe_inner_text(listing, '[data-testid="baths"]')
        url = await _safe_get_href(listing, '[data-testid="property-card-link"]')

        page_data.append(
            {
                "Title": title,
                "Price": price,
                "Location": location,
                "Bedrooms": bedrooms,
                "Bathrooms": bathrooms,
                "URL": url,
            }
        )
    return page_data, total_pages


async def _wait_for_results(page, dump_on_timeout: bool = True) -> None:
    """Wait until either results or an error is visible."""
    try:
        await page.wait_for_selector(
            f"{','.join(PROPERTY_CARD_SELECTORS)}, [data-testid='error-page']",
            timeout=25000,
        )
    except PlaywrightTimeoutError:
        print(" Timed out waiting for listings or error indicator.")
        if dump_on_timeout:
            await _debug_dump(page, prefix="timeout")


async def _safe_inner_text(handle, selector: str) -> Optional[str]:
    element = await handle.query_selector(selector)
    if not element:
        return None
    return await element.inner_text()


async def _safe_get_href(handle, selector: str) -> Optional[str]:
    element = await handle.query_selector(selector)
    if not element:
        return None
    href = await element.get_attribute("href")
    if href and href.startswith("/"):
        return f"https://www.daft.ie{href}"
    return href


async def _query_property_cards(page):
    for selector in PROPERTY_CARD_SELECTORS:
        cards = await page.query_selector_all(selector)
        if cards:
            return cards
    html = (await page.content()).lower()
    if any(snippet in html for snippet in BLOCK_TEXT_SNIPPETS):
        print(" Block page detected while querying property cards.")
    return []


def parse_listing_summaries_from_html(html: str) -> Tuple[List[Dict[str, Optional[str]]], Optional[int]]:
    soup = BeautifulSoup(html, "html.parser")
    script = soup.find("script", id="__NEXT_DATA__")
    if not script or not script.string:
        return [], None
    try:
        payload = json.loads(script.string)
    except json.JSONDecodeError:
        return [], None

    search_data = _extract_listings_payload(payload)
    if not search_data:
        return [], None

    listings: List[Dict[str, Optional[str]]] = []
    results = search_data.get("results") or search_data.get("listings") or []
    print(f"    JSON payload contains {len(results)} entries")
    for entry in results:
        listing = entry.get("listing") if isinstance(entry, dict) else None
        if listing is None:
            listing = entry
        if not isinstance(listing, dict):
            continue

        listings.append(
            {
                "Title": _coerce_to_str(
                    listing.get("title") or listing.get("headline") or listing.get("seoTitle")
                ),
                "Price": _extract_price(listing),
                "Location": _extract_location(listing),
                "Bedrooms": _coerce_to_str(
                    listing.get("beds")
                    or listing.get("bedrooms")
                    or listing.get("numBedrooms")
                    or listing.get("bedroomCount")
                ),
                "Bathrooms": _coerce_to_str(
                    listing.get("baths")
                    or listing.get("bathrooms")
                    or listing.get("numBathrooms")
                    or listing.get("bathroomCount")
                ),
                "URL": _build_url(listing, entry),
                "ListingId": _coerce_to_str(listing.get("id")),
            }
        )
    total_pages = _extract_total_pages(search_data)
    return listings, total_pages


def _extract_listings_payload(payload: Any) -> Optional[Dict[str, Any]]:
    if isinstance(payload, dict):
        props = payload.get("props", {})
        if isinstance(props, dict):
            page_props = props.get("pageProps", {})
            if isinstance(page_props, dict):
                for key in ("searchData", "listings"):
                    data = page_props.get(key)
                    if isinstance(data, dict):
                        return data
                    if key == "listings" and isinstance(data, list):
                        return {"results": data, "pagination": page_props.get("paging")}
        for value in payload.values():
            found = _extract_listings_payload(value)
            if found:
                return found
    elif isinstance(payload, list):
        for item in payload:
            found = _extract_listings_payload(item)
            if found:
                return found
    return None


def _extract_price(listing: Dict[str, Any]) -> Optional[str]:
    price = listing.get("price")
    if isinstance(price, dict):
        for key in ("displayValue", "display", "value"):
            if price.get(key):
                return _coerce_to_str(price.get(key))
    return _coerce_to_str(price)


def _extract_location(listing: Dict[str, Any]) -> Optional[str]:
    address = listing.get("address")
    if isinstance(address, dict):
        for key in ("line1", "formattedAddress"):
            if address.get(key):
                return _coerce_to_str(address.get(key))
    for key in ("displayAddress", "shortAddress", "location"):
        if listing.get(key):
            return _coerce_to_str(listing.get(key))
    return None


def _build_url(listing: Dict[str, Any], entry: Any) -> Optional[str]:
    url_path = (
        listing.get("seoFriendlyUrl")
        or listing.get("shortUrl")
        or listing.get("canonicalUrl")
        or listing.get("slug")
        or listing.get("seoFriendlyPath")
    )
    if not url_path and isinstance(entry, dict):
        url_path = entry.get("seoFriendlyUrl") or entry.get("url") or entry.get("seoFriendlyPath")
    if not url_path:
        return None
    if isinstance(url_path, str) and url_path.startswith("/"):
        return f"https://www.daft.ie{url_path}"
    return _coerce_to_str(url_path)


def _extract_total_pages(search_data: Dict[str, Any]) -> Optional[int]:
    pagination = search_data.get("pagination")
    if not isinstance(pagination, dict):
        return None
    for key in ("totalPages", "total", "lastPage", "last", "pages"):
        value = pagination.get(key)
        if value is None:
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return None


def _coerce_to_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_label(label: str) -> str:
    return re.sub(r"[^0-9A-Za-z]+", "_", label.strip()).strip("_") or "Field"


def _clean_html(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    text = BeautifulSoup(value, "html.parser").get_text(" ", strip=True)
    return text or None


def _format_timestamp(value: Any) -> Optional[str]:
    if value is None:
        return None
    try:
        millis = int(value)
    except (TypeError, ValueError):
        return None
    if millis <= 0:
        return None
    return datetime.fromtimestamp(millis / 1000, tz=timezone.utc).isoformat()


def _build_detail_record(listing: Dict[str, Any], url: str, page_num: int) -> Dict[str, Optional[str]]:
    record: Dict[str, Optional[str]] = {
        "URL": url,
        "PageNumber": page_num,
        "ListingId": _coerce_to_str(listing.get("id")),
        "Title": _coerce_to_str(listing.get("title")),
        "Price": _extract_price(listing),
        "Bedrooms": _coerce_to_str(listing.get("numBedrooms")),
        "Bathrooms": _coerce_to_str(listing.get("numBathrooms")),
        "PropertyType": _coerce_to_str(listing.get("propertyType")),
        "Shortcode": _coerce_to_str(listing.get("daftShortcode")),
        "FeaturedLevel": _coerce_to_str(listing.get("featuredLevel")),
        "PublishDate": _format_timestamp(listing.get("publishDate")),
        "LastUpdateDate": _format_timestamp(listing.get("lastUpdateDate")),
        "Description": _clean_html(listing.get("description")),
        "AreaName": _coerce_to_str(listing.get("areaName")),
        "DateOfConstruction": _coerce_to_str(listing.get("dateOfConstruction")),
        "Sections": ", ".join(listing.get("sections", []) or []),
        "SaleTypes": ", ".join(listing.get("saleType", []) or []),
        "IsPremierPartner": str(listing.get("premierPartner")) if listing.get("premierPartner") is not None else None,
        "IsRepublicOfIreland": str(listing.get("isInRepublicOfIreland")) if listing.get("isInRepublicOfIreland") is not None else None,
    }

    point = listing.get("point", {})
    if isinstance(point, dict):
        coords = point.get("coordinates")
        if isinstance(coords, (list, tuple)) and len(coords) == 2:
            record["Longitude"] = _coerce_to_str(coords[0])
            record["Latitude"] = _coerce_to_str(coords[1])

    non_formatted = listing.get("nonFormatted") or {}
    if isinstance(non_formatted, dict):
        record["BedroomsNumeric"] = _coerce_to_str(non_formatted.get("beds"))
        record["PriceNumeric"] = _coerce_to_str(non_formatted.get("price"))
        record["Section"] = _coerce_to_str(non_formatted.get("section"))

    facilities = listing.get("facilities") or []
    if facilities:
        names = sorted({facility.get("name") for facility in facilities if facility.get("name")})
        if names:
            record["Facilities"] = ", ".join(names)

    ber = listing.get("ber") or {}
    if isinstance(ber, dict):
        record["BER"] = _coerce_to_str(ber.get("rating"))

    seller = listing.get("seller") or {}
    if isinstance(seller, dict):
        record["AgentName"] = _coerce_to_str(seller.get("name"))
        record["AgentBranch"] = _coerce_to_str(seller.get("branch"))
        record["AgentAddress"] = _coerce_to_str(seller.get("address"))
        record["AgentLicence"] = _coerce_to_str(seller.get("licenceNumber"))
        record["AgentType"] = _coerce_to_str(seller.get("sellerType"))

    media = listing.get("media") or {}
    if isinstance(media, dict):
        images = media.get("images") or []
        record["ImageCount"] = _coerce_to_str(len(images) if isinstance(images, list) else None)
        videos = media.get("videos") or []
        record["HasVideo"] = str(bool(videos)) if videos is not None else None

    overview = listing.get("propertyOverview") or []
    if isinstance(overview, list):
        for item in overview:
            if not isinstance(item, dict):
                continue
            label = item.get("label")
            text = item.get("text") or item.get("value")
            if not label or text is None:
                continue
            record[f"Overview_{_normalize_label(label)}"] = _coerce_to_str(text)

    prs = listing.get("prs")
    if isinstance(prs, dict):
        record["PRS_TotalUnitTypes"] = _coerce_to_str(prs.get("totalUnitTypes"))
        parent = prs.get("parentDevelopment")
        if isinstance(parent, dict):
            record["DevelopmentTitle"] = _coerce_to_str(parent.get("title"))
            record["DevelopmentURL"] = _build_url(parent, parent)

    return record


def _extract_first_number(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    match = re.search(r"([0-9]+(?:\.[0-9]+)?)", value.replace(",", ""))
    if not match:
        return None
    return match.group(1)


def _extract_price_numeric_from_text(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    digits = re.sub(r"[^\d]", "", value)
    return digits or None


def _extract_sub_unit_summaries(
    listing: Dict[str, Any], parent_url: str, page_num: int
) -> List[Dict[str, Optional[str]]]:
    sub_units: List[Dict[str, Optional[str]]] = []
    prs = listing.get("prs")
    if not isinstance(prs, dict):
        return sub_units

    raw_units = prs.get("subUnits") or []
    if not isinstance(raw_units, list):
        return sub_units

    for unit in raw_units:
        if not isinstance(unit, dict):
            continue
        unit_url = _build_url(unit, unit)
        price_text = _coerce_to_str(unit.get("price"))
        bedrooms_text = _coerce_to_str(unit.get("numBedrooms"))
        bathrooms_text = _coerce_to_str(unit.get("numBathrooms"))
        image = unit.get("image") if isinstance(unit, dict) else None
        primary_image = None
        if isinstance(image, dict):
            for key in ("size720x480", "size680x392", "size600x600", "size400x300", "size320x280", "size1440x960"):
                if image.get(key):
                    primary_image = _coerce_to_str(image.get(key))
                    break
        sub_units.append(
            {
                "ParentURL": parent_url,
                "URL": unit_url,
                "ListingId": _coerce_to_str(unit.get("id")),
                "Price": price_text,
                "PriceNumeric": _extract_price_numeric_from_text(price_text),
                "Bedrooms": bedrooms_text,
                "BedroomsNumeric": _extract_first_number(bedrooms_text),
                "Bathrooms": bathrooms_text,
                "BathroomsNumeric": _extract_first_number(bathrooms_text),
                "PropertyType": _coerce_to_str(unit.get("propertyType")),
                "Category": _coerce_to_str(unit.get("category")),
                "PageNumber": str(page_num),
                "IsSubUnit": "True",
                "ImageURL": primary_image,
                "VirtualTour": _coerce_to_str(image.get("caption") if isinstance(image, dict) else None),
            }
        )

    return sub_units


def parse_listing_detail_html(html: str, url: str, page_num: int) -> Tuple[Optional[Dict[str, Optional[str]]], List[Dict[str, Optional[str]]]]:
    soup = BeautifulSoup(html, "html.parser")
    script = soup.find("script", id="__NEXT_DATA__")
    if not script or not script.string:
        return None, []
    try:
        payload = json.loads(script.string)
    except json.JSONDecodeError:
        return None, []
    listing = payload.get("props", {}).get("pageProps", {}).get("listing")
    if not isinstance(listing, dict):
        return None, []

    detail = _build_detail_record(listing, url, page_num)
    sub_units = _extract_sub_unit_summaries(listing, url, page_num)
    return detail, sub_units


async def _fetch_listing_detail_playwright(
    detail_page,
    summary: Dict[str, Optional[str]],
    page_num: int,
    *,
    is_sub_unit: bool = False,
    parent: Optional[Dict[str, Optional[str]]] = None,
) -> List[Dict[str, Optional[str]]]:
    url = summary.get("URL")
    if not url:
        fallback = dict(summary)
        fallback.setdefault("IsSubUnit", "True" if is_sub_unit else "False")
        if parent:
            fallback.setdefault("ParentListingId", parent.get("ListingId"))
            fallback.setdefault("ParentURL", parent.get("URL"))
        return [fallback]

    try:
        await detail_page.goto(url, wait_until="domcontentloaded", timeout=45000)
        await accept_cookie_banner(detail_page)
        html = await detail_page.content()
    except PlaywrightTimeoutError:
        print(f" Timeout loading detail page: {url}")
        await _debug_dump(detail_page, f"detail_timeout_{int(time.time())}")
        fallback = dict(summary)
        fallback.setdefault("IsSubUnit", "True" if is_sub_unit else "False")
        if parent:
            fallback.setdefault("ParentListingId", parent.get("ListingId"))
            fallback.setdefault("ParentURL", parent.get("URL"))
        return [fallback]
    except PlaywrightError as exc:
        print(f" Playwright error loading detail page: {url} ({exc})")
        await _debug_dump(detail_page, f"detail_error_{int(time.time())}")
        fallback = dict(summary)
        fallback.setdefault("IsSubUnit", "True" if is_sub_unit else "False")
        if parent:
            fallback.setdefault("ParentListingId", parent.get("ListingId"))
            fallback.setdefault("ParentURL", parent.get("URL"))
        return [fallback]

    detail, sub_units = parse_listing_detail_html(html, url, page_num)
    if detail is None:
        print(f" Could not parse detail page: {url}")
        fallback = dict(summary)
        fallback.setdefault("IsSubUnit", "True" if is_sub_unit else "False")
        if parent:
            fallback.setdefault("ParentListingId", parent.get("ListingId"))
            fallback.setdefault("ParentURL", parent.get("URL"))
        return [fallback]

    merged = {**summary, **detail}
    merged["IsSubUnit"] = "True" if is_sub_unit else "False"
    if parent:
        merged.setdefault("ParentListingId", parent.get("ListingId"))
        merged.setdefault("ParentURL", parent.get("URL"))

    records: List[Dict[str, Optional[str]]] = [merged]

    if not is_sub_unit and sub_units:
        parent_meta = {
            "ListingId": merged.get("ListingId"),
            "URL": merged.get("URL") or url,
            "Title": merged.get("Title"),
        }
        for sub in sub_units:
            sub_url = sub.get("URL")
            if not sub_url or sub_url == url:
                # avoid recursion loops or empty sub units
                sub_record = {**merged, **sub}
                sub_record["IsSubUnit"] = "True"
                sub_record.setdefault("ParentListingId", parent_meta.get("ListingId"))
                sub_record.setdefault("ParentURL", parent_meta.get("URL"))
                records.append(sub_record)
                continue

            sub_summary = dict(sub)
            sub_summary.setdefault("Title", merged.get("Title"))
            sub_summary.setdefault("DevelopmentTitle", merged.get("DevelopmentTitle"))
            sub_summary.setdefault("PageNumber", str(page_num))
            sub_summary.setdefault("ParentListingId", parent_meta.get("ListingId"))
            sub_summary.setdefault("ParentURL", parent_meta.get("URL"))
            sub_summary["IsSubUnit"] = "True"

            sub_records = await _fetch_listing_detail_playwright(
                detail_page,
                sub_summary,
                page_num,
                is_sub_unit=True,
                parent=merged,
            )
            records.extend(sub_records)

    return records


async def _gather_detail_records_playwright(context, summaries: List[Dict[str, Optional[str]]]) -> List[Dict[str, Optional[str]]]:
    results: List[Dict[str, Optional[str]]] = []
    seen: set[str] = set()
    if not summaries:
        return results

    queue: asyncio.Queue = asyncio.Queue()
    for summary in summaries:
        url = summary.get("URL")
        if not url or url in seen:
            continue
        seen.add(url)
        queue.put_nowait(summary)

    concurrency = min(3, queue.qsize()) or 1  # keep concurrency modest to lower block risk
    for _ in range(concurrency):
        queue.put_nowait(None)

    async def worker():
        detail_page = await context.new_page()
        try:
            while True:
                item = await queue.get()
                if item is None:
                    break
                summary = item
                url = summary.get("URL")
                if not url:
                    results.append(summary)
                    continue
                page_num = summary.get("PageNumber") or 1
                detail_records = await _fetch_listing_detail_playwright(
                    detail_page,
                    summary,
                    int(page_num),
                )
                if detail_records:
                    results.extend(detail_records)
                else:
                    results.append(summary)
                if detail_page.is_closed():
                    detail_page = await context.new_page()
                await asyncio.sleep(random.uniform(0.12, 0.3))
        finally:
            try:
                if not detail_page.is_closed():
                    await detail_page.close()
            except PlaywrightError:
                pass

    tasks = [asyncio.create_task(worker()) for _ in range(concurrency)]
    await asyncio.gather(*tasks)
    return results


async def _fetch_search_page_playwright(page, page_num: int) -> List[Dict[str, Optional[str]]]:
    target_url = BASE_URL if page_num == 1 else f"{BASE_URL}?page={page_num}"
    if page_num != 1:
        try:
            await page.goto(target_url, wait_until="domcontentloaded", timeout=45000)
        except PlaywrightTimeoutError:
            print(f" Navigation timeout for {target_url}; continuing with current content.")
        await accept_cookie_banner(page)
        try:
            await page.wait_for_load_state("networkidle", timeout=25000)
        except PlaywrightTimeoutError:
            print(" Network idle timeout after navigation; proceeding with DOM snapshot.")
        await asyncio.sleep(random.uniform(0.15, 0.35))

    html = await page.content()
    listings, _ = parse_listing_summaries_from_html(html)
    if not listings:
        await _wait_for_results(page)
        html = await page.content()
        listings, _ = parse_listing_summaries_from_html(html)
        if not listings:
            await _debug_dump(page, f"page_{page_num}_empty")

    for record in listings:
        record.setdefault("PageNumber", page_num)
    return listings


async def determine_total_pages(page) -> int:
    try:
        await page.wait_for_selector('[data-testid="pagination"]', timeout=10000)
        pagination_items = await page.query_selector_all('[data-testid="pagination-list"] li')
        numbers = []
        for item in pagination_items:
            text = (await item.inner_text()).strip()
            if text.isdigit():
                numbers.append(int(text))
        return max(numbers) if numbers else 1
    except PlaywrightTimeoutError:
        return 1


async def accept_cookie_banner(page) -> None:
    if callable(getattr(page, "is_closed", None)) and page.is_closed():
        return
    if getattr(page, "_cookies_dismissed", False):
        return
    context = getattr(page, "context", None)
    if context and getattr(context, "_cookies_dismissed", False):
        page._cookies_dismissed = True
        return
    for selector in COOKIE_BUTTON_SELECTORS:
        try:
            button = await page.wait_for_selector(selector, timeout=5000)
            if button:
                await button.click()
                await page.wait_for_timeout(500)
                print(" Cookie banner dismissed.")
                page._cookies_dismissed = True
                if context:
                    context._cookies_dismissed = True
                return
        except TimeoutError:
            continue
        except PlaywrightTimeoutError:
            continue
        except PlaywrightError:
            return

    # iframe banners
    for frame in page.frames:
        for selector in COOKIE_BUTTON_SELECTORS:
            try:
                button = await frame.wait_for_selector(selector, timeout=2000)
                if button:
                    await button.click()
                    await page.wait_for_timeout(500)
                    print(" Cookie banner dismissed (iframe).")
                    page._cookies_dismissed = True
                    if context:
                        context._cookies_dismissed = True
                    return
            except PlaywrightTimeoutError:
                continue
            except TimeoutError:
                continue
            except PlaywrightError:
                return


async def _debug_dump(page, prefix: str) -> None:
    timestamp = int(time.time())
    screenshot = f"{prefix}_{timestamp}.png"
    html_path = f"{prefix}_{timestamp}.html"
    try:
        await page.screenshot(path=screenshot, full_page=True)
        print(f"  - Saved screenshot: {screenshot}")
    except Exception:
        pass
    try:
        html_content = await page.content()
        Path(html_path).write_text(html_content, encoding="utf-8")
        print(f"  - Saved HTML snapshot: {html_path}")
    except Exception:
        pass


async def main(headless: bool = False, max_pages: Optional[int] = DEFAULT_MAX_PAGES) -> None:
    initial_listings: List[Dict[str, Optional[str]]] = []
    total_pages = 1
    json_total = None

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless, args=["--disable-blink-features=AutomationControlled"])
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_0) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            viewport={"width": 1440, "height": 900},
        )
        page = await context.new_page()

        print(f" Loading base page: {BASE_URL}")
        try:
            await page.goto(BASE_URL, wait_until="domcontentloaded", timeout=45000)
        except PlaywrightTimeoutError:
            print(" Initial navigation timed out. Attempting again after cookie handling.")
        await accept_cookie_banner(page)
        try:
            await page.wait_for_load_state("networkidle", timeout=30000)
        except PlaywrightTimeoutError:
            print(" Waiting for networkidle timed out; continuing with current content.")
        await _wait_for_results(page, dump_on_timeout=False)

        initial_html = await page.content()
        initial_listings, json_total = parse_listing_summaries_from_html(initial_html)
        total_pages = json_total or await determine_total_pages(page)
        if total_pages is None or total_pages < 1:
            total_pages = 1
        print(f" Total pages found: {total_pages}")

        if not initial_listings:
            print(" Falling back to DOM parsing for the first page.")
            initial_listings, fallback_total = await scrape_page(page, BASE_URL, 1)
            if not json_total and fallback_total:
                total_pages = fallback_total

        for record in initial_listings:
            record.setdefault("PageNumber", 1)

        limit = total_pages
        if max_pages is not None:
            if max_pages < 1:
                max_pages = 1
            limit = min(total_pages, max_pages)
            if limit < total_pages:
                print(f" Limiting scrape to {limit} pages.")

        all_properties: List[Dict[str, Optional[str]]] = list(initial_listings)
        print(f"  - Page 1 yielded {len(initial_listings)} listings so far.")

        if limit > 1:
            for page_num in range(2, limit + 1):
                print(f" Scraping page {page_num} of {limit} ...")
                page_data = await _fetch_search_page_playwright(page, page_num)
                print(f"  - Found {len(page_data)} listings")
                all_properties.extend(page_data)
                await asyncio.sleep(random.uniform(0.3, 0.6))

        if not all_properties:
            print(" No listings were collected.")
            return

        detailed_records = await _gather_detail_records_playwright(context, all_properties)
        if not detailed_records:
            print(" No detailed listing data was collected.")
            return

        await _export_results(detailed_records)

        await browser.close()


async def _export_results(data: List[Dict[str, Optional[str]]]) -> None:
    if pd is None:
        import csv

        filename = "daft_listings.csv"
        keys = sorted({key for record in data for key in record.keys()})
        with open(filename, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=keys)
            writer.writeheader()
            writer.writerows(data)
        print(f" pandas not installed; saved {len(data)} records to {filename}")
        return

    df = pd.DataFrame(data)
    try:
        df.to_excel("daft_listings.xlsx", index=False)
        print(f" Saved {len(data)} records to daft_listings.xlsx")
    except ModuleNotFoundError as exc:
        if "openpyxl" in str(exc):
            filename = "daft_listings.csv"
            df.to_csv("/Users/suley/Desktop/dublin-student-housing/data/raw/daft_listings.csv", index=False)
            print(
                " openpyxl missing; saved results to CSV instead. "
                "Install openpyxl if you prefer Excel output."
            )
        else:
            raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape rental listings from daft.ie using Playwright.")
    parser.add_argument("--headless", action="store_true", help="Run browser in headless mode.")
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help=f"Limit number of pages to scrape (default: {DEFAULT_MAX_PAGES}).",
    )
    args = parser.parse_args()

    # Headless default: environment variable overrides flag
    env_headless = os.environ.get("HEADLESS", "").lower() in {"1", "true", "yes"}
    headless = args.headless or env_headless
    max_pages = args.max_pages if args.max_pages is not None else DEFAULT_MAX_PAGES

    asyncio.run(main(headless=headless, max_pages=max_pages))
