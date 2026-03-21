"""
property_scraper.py — Property price scraper for MagicBricks / 99acres.

Scrapes Bengaluru property listings to extract price_sqft, area, bedrooms,
and locality. Used as a VALIDATION LABEL only (not an ML input feature).

Note: This scraper uses requests + BeautifulSoup for simplicity.
For production, consider using Scrapy for better rate-limiting and proxy support.
"""

import re
import time
import logging
from datetime import date
from typing import Optional

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# Bengaluru localities to scrape
BENGALURU_LOCALITIES = [
    "whitefield", "electronic-city", "sarjapur-road", "marathahalli",
    "hsr-layout", "koramangala", "indiranagar", "jayanagar",
    "jp-nagar", "bannerghatta-road", "hebbal", "yelahanka",
    "rajajinagar", "malleshwaram", "basavanagudi", "btm-layout",
    "silk-board", "kr-puram", "rt-nagar", "vijayanagar",
    "kengeri", "banashankari", "nagarbhavi", "rr-nagar",
    "hennur", "thanisandra", "yelahanka-new-town", "devanahalli",
    "bellandur", "varthur",
]

MAX_RETRIES = 3
REQUEST_DELAY = 2  # seconds between requests


class PropertyScraper:
    """Scraper for MagicBricks and 99acres property listings."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9",
        })

    def scrape_magicbricks(self, locality: str,
                           max_pages: int = 3) -> list[dict]:
        """
        Scrape property listings from MagicBricks for a given locality.

        Args:
            locality: Locality slug (e.g., 'koramangala')
            max_pages: Maximum pages to scrape

        Returns:
            List of property listing dicts
        """
        listings = []
        base_url = (
            f"https://www.magicbricks.com/property-for-sale/residential-real-estate"
            f"?bedroom=&proptype=Multistorey-Apartment,Builder-Floor-Apartment,"
            f"Penthouse,Studio-Apartment&cityName=Bangalore"
            f"&BudgetMin=&BudgetMax=&localty={locality}"
        )

        for page in range(1, max_pages + 1):
            url = f"{base_url}&page={page}"
            html = self._fetch_page(url)
            if not html:
                break

            page_listings = self._parse_magicbricks_page(html, locality)
            if not page_listings:
                break

            listings.extend(page_listings)
            time.sleep(REQUEST_DELAY)

        logger.info(
            f"Scraped {len(listings)} listings from MagicBricks "
            f"for {locality}"
        )
        return listings

    def scrape_99acres(self, locality: str,
                       max_pages: int = 3) -> list[dict]:
        """
        Scrape property listings from 99acres for a given locality.

        Args:
            locality: Locality slug
            max_pages: Maximum pages to scrape

        Returns:
            List of property listing dicts
        """
        listings = []
        base_url = (
            f"https://www.99acres.com/search/property/buy/{locality}"
            f"-bangalore?city=97&keyword={locality}"
        )

        for page in range(1, max_pages + 1):
            url = f"{base_url}&page={page}"
            html = self._fetch_page(url)
            if not html:
                break

            page_listings = self._parse_99acres_page(html, locality)
            if not page_listings:
                break

            listings.extend(page_listings)
            time.sleep(REQUEST_DELAY)

        logger.info(
            f"Scraped {len(listings)} listings from 99acres for {locality}"
        )
        return listings

    def scrape_all_localities(self, source: str = "magicbricks",
                              max_pages: int = 2) -> list[dict]:
        """
        Scrape all Bengaluru localities.

        Args:
            source: 'magicbricks' or '99acres'
            max_pages: Pages per locality

        Returns:
            All listings across all localities
        """
        all_listings = []

        for locality in BENGALURU_LOCALITIES:
            logger.info(f"Scraping {source} for {locality}...")
            try:
                if source == "magicbricks":
                    listings = self.scrape_magicbricks(locality, max_pages)
                else:
                    listings = self.scrape_99acres(locality, max_pages)
                all_listings.extend(listings)
            except Exception as e:
                logger.error(f"Error scraping {locality}: {e}")
                continue

            time.sleep(REQUEST_DELAY)

        logger.info(
            f"Total: {len(all_listings)} listings from {source}"
        )
        return all_listings

    def _fetch_page(self, url: str) -> Optional[str]:
        """Fetch HTML page with retry logic."""
        for attempt in range(MAX_RETRIES):
            try:
                response = self.session.get(url, timeout=30)
                if response.status_code == 200:
                    return response.text
                elif response.status_code == 429:
                    wait = (attempt + 1) * 5
                    logger.warning(f"Rate limited, waiting {wait}s...")
                    time.sleep(wait)
                else:
                    logger.warning(
                        f"HTTP {response.status_code} for {url}"
                    )
                    return None
            except requests.RequestException as e:
                logger.warning(f"Request failed (attempt {attempt + 1}): {e}")
                time.sleep(2 ** attempt)

        return None

    def _parse_magicbricks_page(self, html: str,
                                 locality: str) -> list[dict]:
        """Parse MagicBricks search results page."""
        soup = BeautifulSoup(html, "html.parser")
        listings = []

        # MagicBricks property cards
        cards = soup.find_all("div", class_=re.compile(r"mb-srp__card"))

        for card in cards:
            try:
                listing = self._extract_magicbricks_card(card, locality)
                if listing:
                    listings.append(listing)
            except Exception as e:
                logger.debug(f"Failed to parse card: {e}")
                continue

        return listings

    def _extract_magicbricks_card(self, card, locality: str) -> Optional[dict]:
        """Extract listing data from a MagicBricks property card."""
        # Price
        price_el = card.find(class_=re.compile(r"mb-srp__card__price"))
        price_text = price_el.get_text(strip=True) if price_el else ""
        price_total = self._parse_price(price_text)

        # Area
        area_el = card.find(class_=re.compile(r"mb-srp__card__summary--value"))
        area_text = area_el.get_text(strip=True) if area_el else ""
        area_sqft = self._parse_area(area_text)

        # Bedrooms
        config_el = card.find(class_=re.compile(r"mb-srp__card__summary"))
        bedrooms = self._parse_bedrooms(
            config_el.get_text() if config_el else ""
        )

        # Property type
        prop_type_el = card.find(
            class_=re.compile(r"mb-srp__card__property--type")
        )
        prop_type = prop_type_el.get_text(strip=True) if prop_type_el else ""

        # Compute price per sqft
        price_sqft = None
        if price_total and area_sqft and area_sqft > 0:
            price_sqft = round(price_total / area_sqft, 2)

        if not price_total and not price_sqft:
            return None

        return {
            "listing_id": None,
            "source": "magicbricks",
            "locality": locality.replace("-", " ").title(),
            "ward_name": None,  # Will be resolved via spatial join
            "city": "Bengaluru",
            "price_total": price_total,
            "price_sqft": price_sqft,
            "area_sqft": area_sqft,
            "bedrooms": bedrooms,
            "property_type": prop_type,
            "listing_date": date.today().isoformat(),
        }

    def _parse_99acres_page(self, html: str,
                             locality: str) -> list[dict]:
        """Parse 99acres search results page."""
        soup = BeautifulSoup(html, "html.parser")
        listings = []

        cards = soup.find_all("div", class_=re.compile(r"srp__card"))

        for card in cards:
            try:
                listing = self._extract_99acres_card(card, locality)
                if listing:
                    listings.append(listing)
            except Exception as e:
                logger.debug(f"Failed to parse 99acres card: {e}")
                continue

        return listings

    def _extract_99acres_card(self, card, locality: str) -> Optional[dict]:
        """Extract listing data from a 99acres property card."""
        price_el = card.find(class_=re.compile(r"list_header_semiBold"))
        price_text = price_el.get_text(strip=True) if price_el else ""
        price_total = self._parse_price(price_text)

        area_el = card.find(string=re.compile(r"sq\.?\s*ft", re.I))
        area_sqft = self._parse_area(
            area_el.parent.get_text() if area_el else ""
        )

        config_text = card.get_text()
        bedrooms = self._parse_bedrooms(config_text)

        price_sqft = None
        if price_total and area_sqft and area_sqft > 0:
            price_sqft = round(price_total / area_sqft, 2)

        if not price_total and not price_sqft:
            return None

        return {
            "listing_id": None,
            "source": "99acres",
            "locality": locality.replace("-", " ").title(),
            "ward_name": None,
            "city": "Bengaluru",
            "price_total": price_total,
            "price_sqft": price_sqft,
            "area_sqft": area_sqft,
            "bedrooms": bedrooms,
            "property_type": "",
            "listing_date": date.today().isoformat(),
        }

    @staticmethod
    def _parse_price(text: str) -> Optional[float]:
        """Parse price from strings like '₹ 1.25 Cr', '₹ 85 Lac'."""
        if not text:
            return None
        text = text.replace(",", "").replace("₹", "").strip()

        cr_match = re.search(r"([\d.]+)\s*cr", text, re.I)
        if cr_match:
            return float(cr_match.group(1)) * 10_000_000

        lac_match = re.search(r"([\d.]+)\s*lac|lakh", text, re.I)
        if lac_match:
            return float(lac_match.group(1)) * 100_000

        num_match = re.search(r"([\d.]+)", text)
        if num_match:
            return float(num_match.group(1))

        return None

    @staticmethod
    def _parse_area(text: str) -> Optional[float]:
        """Parse area in sq ft from text."""
        if not text:
            return None
        match = re.search(r"([\d,]+\.?\d*)\s*(?:sq\.?\s*ft|sqft)", text, re.I)
        if match:
            return float(match.group(1).replace(",", ""))
        return None

    @staticmethod
    def _parse_bedrooms(text: str) -> Optional[int]:
        """Parse number of bedrooms from text like '3 BHK'."""
        if not text:
            return None
        match = re.search(r"(\d+)\s*BHK", text, re.I)
        return int(match.group(1)) if match else None


def get_prepare_rows(listings: list[dict]) -> tuple[list[str], list[tuple]]:
    """Prepare listings for bulk DB insertion."""
    columns = [
        "listing_id", "source", "locality", "ward_name", "city",
        "price_total", "price_sqft", "area_sqft", "bedrooms",
        "property_type", "listing_date"
    ]
    rows = [
        (
            l["listing_id"], l["source"], l["locality"], l["ward_name"],
            l["city"], l["price_total"], l["price_sqft"], l["area_sqft"],
            l["bedrooms"], l["property_type"], l["listing_date"]
        )
        for l in listings
    ]
    return columns, rows
