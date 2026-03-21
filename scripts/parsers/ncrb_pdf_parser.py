"""
ncrb_pdf_parser.py — Parse NCRB Crime in India PDF reports using pdfplumber.

Extracts IPC offense counts by city, year, and offense type from
NCRB "Crime in India" annual publications.

Usage:
    from scripts.parsers.ncrb_pdf_parser import parse_ncrb_pdf
    records = parse_ncrb_pdf("data/ncrb/Crime_in_India_2022.pdf", year=2022)
"""

import re
import logging
from pathlib import Path
from typing import Optional

import pdfplumber

logger = logging.getLogger(__name__)

# IPC offense categories we care about
TARGET_OFFENSES = {
    "murder": ["murder", "culpable homicide"],
    "theft": ["theft", "auto theft", "vehicle theft"],
    "robbery": ["robbery", "dacoity"],
    "assault": ["assault", "hurt", "grievous hurt"],
    "kidnapping": ["kidnapping", "abduction"],
    "burglary": ["burglary", "housebreaking", "house-breaking"],
    "crimes against women": ["rape", "dowry death", "cruelty by husband"],
    "economic offenses": ["cheating", "forgery", "criminal breach of trust"],
    "cyber crimes": ["cyber crime", "information technology act"],
}

# Cities we're targeting (Bengaluru focus, but parse all metro cities)
TARGET_CITIES = [
    "bengaluru", "bangalore", "delhi", "mumbai", "chennai",
    "hyderabad", "kolkata", "pune", "ahmedabad", "jaipur",
]


def parse_ncrb_pdf(pdf_path: str, year: int,
                   target_city: str = "bengaluru") -> list[dict]:
    """
    Parse an NCRB Crime in India PDF and extract offense data.

    Strategy:
      1. Iterate through pages looking for tables
      2. Identify tables containing city-level crime data
      3. Extract offense type, city, and count columns
      4. Filter for target cities and normalize offense categories

    Args:
        pdf_path: Path to the NCRB PDF file
        year: Year of the report
        target_city: Primary city to filter for (default: bengaluru)

    Returns:
        List of dicts with keys:
            state, city, year, offense_category, offense_type, count,
            source_pdf, page_number
    """
    pdf_path = Path(pdf_path)
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF not found: {pdf_path}")

    records = []
    logger.info(f"Parsing NCRB PDF: {pdf_path.name} (year={year})")

    with pdfplumber.open(str(pdf_path)) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            tables = page.extract_tables()

            if not tables:
                continue

            for table in tables:
                page_records = _parse_crime_table(
                    table, year, str(pdf_path.name), page_num
                )
                records.extend(page_records)

    # Filter for target city
    if target_city:
        city_pattern = target_city.lower()
        filtered = [
            r for r in records
            if city_pattern in r["city"].lower()
        ]
        logger.info(
            f"Filtered {len(filtered)}/{len(records)} records for {target_city}"
        )
        records = filtered

    logger.info(f"Parsed {len(records)} crime records from {pdf_path.name}")
    return records


def _parse_crime_table(table: list[list], year: int,
                       source_pdf: str, page_number: int) -> list[dict]:
    """
    Parse a single extracted table from pdfplumber.

    The typical NCRB table structure:
      Row 0: Headers (Sl.No, City, [Offense columns]...)
      Row 1+: Data rows

    Args:
        table: 2D list from pdfplumber.extract_tables()
        year: Report year
        source_pdf: Source PDF filename
        page_number: Page number

    Returns:
        List of parsed record dicts
    """
    if not table or len(table) < 2:
        return []

    records = []
    headers = _clean_headers(table[0])

    # Identify the city column
    city_col_idx = _find_column_index(headers, ["city", "cities", "name of city"])
    if city_col_idx is None:
        return []

    # Find offense columns
    offense_mappings = _map_offense_columns(headers)
    if not offense_mappings:
        return []

    for row_idx, row in enumerate(table[1:], start=1):
        if not row or len(row) <= city_col_idx:
            continue

        city = _clean_text(row[city_col_idx])
        if not city or not _is_target_city(city):
            continue

        state = _extract_state(row, headers)

        for offense_category, col_idx in offense_mappings.items():
            if col_idx >= len(row):
                continue

            count = _parse_count(row[col_idx])
            if count is None:
                continue

            records.append({
                "state": state or "Karnataka",
                "city": _normalize_city_name(city),
                "year": year,
                "offense_category": offense_category,
                "offense_type": offense_category,
                "count": count,
                "source_pdf": source_pdf,
                "page_number": page_number,
            })

    return records


def _clean_headers(header_row: list) -> list[str]:
    """Clean and normalize table header values."""
    return [_clean_text(h).lower() if h else "" for h in header_row]


def _clean_text(text: Optional[str]) -> str:
    """Clean extracted text: strip, remove extra whitespace."""
    if not text:
        return ""
    text = re.sub(r"\s+", " ", str(text).strip())
    text = re.sub(r"[^\w\s\-\.\,]", "", text)
    return text


def _find_column_index(headers: list[str],
                       search_terms: list[str]) -> Optional[int]:
    """Find column index matching any of the search terms."""
    for idx, header in enumerate(headers):
        for term in search_terms:
            if term in header:
                return idx
    return None


def _map_offense_columns(headers: list[str]) -> dict[str, int]:
    """
    Map offense categories to column indices based on header text.

    Returns:
        Dict of {offense_category: column_index}
    """
    mappings = {}
    for category, keywords in TARGET_OFFENSES.items():
        for idx, header in enumerate(headers):
            if any(kw in header for kw in keywords):
                mappings[category] = idx
                break
    return mappings


def _is_target_city(city: str) -> bool:
    """Check if the city is one we care about."""
    city_lower = city.lower().strip()
    return any(tc in city_lower for tc in TARGET_CITIES)


def _normalize_city_name(city: str) -> str:
    """Normalize city name to standard form."""
    city_lower = city.lower().strip()
    if "bengaluru" in city_lower or "bangalore" in city_lower:
        return "Bengaluru"
    return city.strip().title()


def _extract_state(row: list, headers: list[str]) -> Optional[str]:
    """Try to extract state name from the row."""
    state_col = _find_column_index(headers, ["state", "state/ut"])
    if state_col is not None and state_col < len(row):
        return _clean_text(row[state_col]) or None
    return None


def _parse_count(value) -> Optional[int]:
    """Parse a count value from table cell, handling various formats."""
    if value is None:
        return None

    text = str(value).strip()
    text = text.replace(",", "").replace(" ", "")

    # Remove any non-numeric characters except minus sign
    text = re.sub(r"[^\d\-]", "", text)

    if not text or text == "-":
        return 0

    try:
        return int(text)
    except ValueError:
        return None


def get_prepare_rows(records: list[dict]) -> tuple[list[str], list[tuple]]:
    """
    Prepare parsed records for bulk DB insertion.

    Returns:
        Tuple of (column_names, row_tuples)
    """
    columns = [
        "state", "city", "year", "offense_category", "offense_type",
        "count", "source_pdf", "page_number"
    ]
    rows = [
        (
            r["state"], r["city"], r["year"], r["offense_category"],
            r["offense_type"], r["count"], r["source_pdf"], r["page_number"]
        )
        for r in records
    ]
    return columns, rows
