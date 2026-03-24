"""
test_ncrb_parser.py — Unit tests for the NCRB PDF parser.

Tests parsing logic, text cleaning, price/count parsing, and
record preparation without needing actual PDF files.
"""

import pytest
from scripts.parsers.ncrb_pdf_parser import (
    _clean_text,
    _parse_count,
    _normalize_city_name,
    _is_target_city,
    _find_column_index,
    _map_offense_columns,
    _parse_crime_table,
    get_prepare_rows,
)


class TestCleanText:
    def test_strips_whitespace(self):
        assert _clean_text("  hello  ") == "hello"

    def test_normalizes_spaces(self):
        assert _clean_text("hello   world") == "hello world"

    def test_handles_none(self):
        assert _clean_text(None) == ""

    def test_handles_empty_string(self):
        assert _clean_text("") == ""


class TestParseCount:
    def test_integer(self):
        assert _parse_count("42") == 42

    def test_with_commas(self):
        assert _parse_count("1,234") == 1234

    def test_with_spaces(self):
        assert _parse_count("1 234") == 1234

    def test_dash(self):
        assert _parse_count("-") == 0

    def test_none(self):
        assert _parse_count(None) is None

    def test_non_numeric(self):
        assert _parse_count("abc") is None

    def test_zero(self):
        assert _parse_count("0") == 0


class TestNormalizeCity:
    def test_bengaluru(self):
        assert _normalize_city_name("Bengaluru") == "Bengaluru"

    def test_bangalore_variant(self):
        assert _normalize_city_name("Bangalore City") == "Bengaluru"

    def test_other_city(self):
        assert _normalize_city_name("mumbai") == "Mumbai"


class TestIsTargetCity:
    def test_bengaluru(self):
        assert _is_target_city("Bengaluru") is True

    def test_bangalore(self):
        assert _is_target_city("Bangalore") is True

    def test_non_target(self):
        assert _is_target_city("Lucknow") is False


class TestFindColumnIndex:
    def test_finds_city(self):
        headers = ["sl.no", "city", "murder", "theft"]
        assert _find_column_index(headers, ["city", "cities"]) == 1

    def test_returns_none_when_missing(self):
        headers = ["sl.no", "state", "murder"]
        assert _find_column_index(headers, ["city"]) is None


class TestMapOffenseColumns:
    def test_maps_multiple_offenses(self):
        headers = ["sl.no", "city", "murder", "theft", "robbery"]
        mappings = _map_offense_columns(headers)
        assert "murder" in mappings
        assert "theft" in mappings
        assert "robbery" in mappings


class TestParseCrimeTable:
    def test_parses_valid_table(self):
        table = [
            ["Sl.No", "City", "Murder", "Theft"],
            ["1", "Bengaluru", "120", "5,432"],
            ["2", "Delhi", "250", "8,901"],
        ]
        records = _parse_crime_table(table, 2022, "test.pdf", 1)
        # Should contain records for both Bengaluru and Delhi
        assert len(records) > 0
        # Verify a Bengaluru record exists
        blr_records = [r for r in records if r["city"] == "Bengaluru"]
        assert len(blr_records) > 0

    def test_empty_table(self):
        records = _parse_crime_table([], 2022, "test.pdf", 1)
        assert records == []

    def test_header_only_table(self):
        records = _parse_crime_table([["City", "Murder"]], 2022, "test.pdf", 1)
        assert records == []


class TestGetPrepareRows:
    def test_formats_correctly(self):
        records = [
            {
                "state": "Karnataka",
                "city": "Bengaluru",
                "year": 2022,
                "offense_category": "murder",
                "offense_type": "murder",
                "count": 120,
                "source_pdf": "test.pdf",
                "page_number": 1,
            }
        ]
        columns, rows = get_prepare_rows(records)
        assert len(columns) == 8
        assert len(rows) == 1
        assert rows[0][0] == "Karnataka"
        assert rows[0][2] == 2022
