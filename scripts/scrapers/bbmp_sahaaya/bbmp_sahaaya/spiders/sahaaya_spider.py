"""
BBMP Sahaaya spider — crawls the BBMP Sahaaya complaint portal
to extract civic complaint records.

The spider navigates the portal's complaint listing pages and
extracts complaint details including ID, category, ward, date, and status.
"""

import re
import logging
from datetime import datetime

import scrapy
from bbmp_sahaaya.items import ComplaintItem

logger = logging.getLogger(__name__)


class SahaayaSpider(scrapy.Spider):
    """Spider for BBMP Sahaaya civic complaints portal."""

    name = "sahaaya"
    allowed_domains = ["sahaaya.bbmp.gov.in", "bbmp.gov.in"]
    start_urls = [
        "https://sahaaya.bbmp.gov.in/complaints/list",
    ]

    custom_settings = {
        "DOWNLOAD_DELAY": 2,
        "CONCURRENT_REQUESTS": 2,
    }

    def __init__(self, start_date=None, end_date=None, *args, **kwargs):
        """
        Initialize spider with optional date range filter.

        Args:
            start_date: Start date string (YYYY-MM-DD)
            end_date: End date string (YYYY-MM-DD)
        """
        super().__init__(*args, **kwargs)
        self.start_date = start_date
        self.end_date = end_date

    def parse(self, response):
        """
        Parse the complaint listing page.

        Extracts complaint summary rows and follows links
        to individual complaint detail pages.
        """
        # Extract complaint rows from the listing table
        rows = response.css("table.complaint-list tbody tr, "
                           "div.complaint-card, "
                           "div.list-item")

        if not rows:
            # Try alternative page structure
            rows = response.css("div[class*='complaint'], "
                               "tr[class*='complaint']")

        for row in rows:
            # Try to extract complaint link
            detail_link = row.css("a::attr(href)").get()
            if detail_link:
                yield response.follow(
                    detail_link,
                    callback=self.parse_complaint_detail,
                    meta={"row_data": self._extract_row_summary(row)},
                )
            else:
                # Extract data directly from the row
                item = self._extract_from_row(row)
                if item:
                    yield item

        # Pagination
        next_page = response.css(
            "a.next-page::attr(href), "
            "a[rel='next']::attr(href), "
            "li.next a::attr(href)"
        ).get()

        if next_page:
            yield response.follow(next_page, callback=self.parse)

    def parse_complaint_detail(self, response):
        """
        Parse individual complaint detail page.

        Extracts full complaint information including description,
        location coordinates, and resolution details.
        """
        item = ComplaintItem()

        # Complaint ID
        item["complaint_id"] = self._extract_field(
            response, ["complaint-id", "ref-number", "tracking-id"]
        )

        # Category & Subcategory
        item["category"] = self._extract_field(
            response, ["category", "complaint-type", "department"]
        )
        item["subcategory"] = self._extract_field(
            response, ["subcategory", "sub-category", "complaint-subtype"]
        )

        # Ward info
        item["ward_name"] = self._extract_field(
            response, ["ward-name", "ward", "location-ward"]
        )
        ward_num = self._extract_field(
            response, ["ward-number", "ward-no"]
        )
        item["ward_number"] = (
            int(ward_num) if ward_num and ward_num.isdigit() else None
        )

        # Date
        date_text = self._extract_field(
            response, ["date-filed", "complaint-date", "created-date"]
        )
        item["date_filed"] = self._parse_date(date_text)

        # Status
        item["status"] = self._extract_field(
            response, ["status", "complaint-status", "current-status"]
        )

        # Description
        item["description"] = self._extract_field(
            response, ["description", "complaint-details", "details"]
        )

        # Coordinates
        lat = self._extract_field(response, ["latitude", "lat"])
        lon = self._extract_field(response, ["longitude", "lng", "lon"])
        item["latitude"] = float(lat) if lat else None
        item["longitude"] = float(lon) if lon else None

        # Merge with row summary if available
        row_data = response.meta.get("row_data", {})
        for key, value in row_data.items():
            if not item.get(key) and value:
                item[key] = value

        # Only yield if we have at minimum a complaint ID
        if item.get("complaint_id"):
            yield item
        else:
            logger.warning(f"Skipping complaint without ID: {response.url}")

    def _extract_row_summary(self, row) -> dict:
        """Extract summary data from a table row or card element."""
        cells = row.css("td")
        data = {}

        if len(cells) >= 4:
            data["complaint_id"] = cells[0].css("::text").get("").strip()
            data["category"] = cells[1].css("::text").get("").strip()
            data["ward_name"] = cells[2].css("::text").get("").strip()
            data["status"] = cells[-1].css("::text").get("").strip()

        return data

    def _extract_from_row(self, row) -> ComplaintItem:
        """Extract a complete complaint item directly from a row."""
        item = ComplaintItem()
        cells = row.css("td")

        if len(cells) < 3:
            return None

        item["complaint_id"] = cells[0].css("::text").get("").strip()
        if not item["complaint_id"]:
            return None

        item["category"] = (
            cells[1].css("::text").get("").strip() if len(cells) > 1 else ""
        )
        item["ward_name"] = (
            cells[2].css("::text").get("").strip() if len(cells) > 2 else ""
        )
        item["date_filed"] = self._parse_date(
            cells[3].css("::text").get("").strip() if len(cells) > 3 else ""
        )
        item["status"] = (
            cells[4].css("::text").get("").strip() if len(cells) > 4 else ""
        )

        item["subcategory"] = ""
        item["ward_number"] = None
        item["description"] = ""
        item["latitude"] = None
        item["longitude"] = None

        return item

    @staticmethod
    def _extract_field(response, class_patterns: list[str]) -> str:
        """
        Extract text from a page using multiple CSS class patterns.

        Tries each pattern until one returns a value.
        """
        for pattern in class_patterns:
            # Try class-based selector
            value = response.css(
                f"[class*='{pattern}']::text, "
                f"[class*='{pattern}'] span::text, "
                f"[id*='{pattern}']::text"
            ).get("")

            if value.strip():
                return value.strip()

            # Try label-based extraction
            label = response.xpath(
                f"//label[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', "
                f"'abcdefghijklmnopqrstuvwxyz'), '{pattern.replace('-', ' ')}')]"
                f"/following-sibling::*[1]/text()"
            ).get("")

            if label.strip():
                return label.strip()

        return ""

    @staticmethod
    def _parse_date(date_text: str) -> str:
        """Parse date string into ISO format."""
        if not date_text:
            return None

        # Try common Indian date formats
        formats = [
            "%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d",
            "%d %b %Y", "%d %B %Y", "%d/%m/%Y %H:%M",
        ]
        for fmt in formats:
            try:
                return datetime.strptime(date_text.strip(), fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue

        return None
