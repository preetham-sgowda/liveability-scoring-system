"""
census_loader.py — Census of India static CSV ingestion.

Loads ward-level census data (population, density, literacy, households)
from cleaned CSV files into raw.census.

Expected CSV columns:
    ward_name, ward_number, population, male_population, female_population,
    area_sqkm, density, literacy_rate, male_literacy, female_literacy,
    household_size, total_households, year
"""

import logging
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

EXPECTED_COLUMNS = [
    "ward_name", "ward_number", "population", "area_sqkm",
    "density", "literacy_rate", "household_size", "year"
]

OPTIONAL_COLUMNS = [
    "male_population", "female_population", "male_literacy",
    "female_literacy", "total_households"
]

COLUMN_TYPES = {
    "ward_name": str,
    "ward_number": "Int64",
    "population": "Int64",
    "male_population": "Int64",
    "female_population": "Int64",
    "area_sqkm": float,
    "density": float,
    "literacy_rate": float,
    "male_literacy": float,
    "female_literacy": float,
    "household_size": float,
    "total_households": "Int64",
    "year": "Int64",
}


def load_census_csv(csv_path: str,
                    year_override: Optional[int] = None) -> pd.DataFrame:
    """
    Load and validate a census CSV file.

    Args:
        csv_path: Path to the census CSV file
        year_override: Override the year column (for files without it)

    Returns:
        Cleaned DataFrame ready for DB insertion
    """
    csv_path = Path(csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(f"Census CSV not found: {csv_path}")

    logger.info(f"Loading census CSV: {csv_path.name}")

    df = pd.read_csv(str(csv_path), encoding="utf-8-sig")

    # Normalize column names
    df.columns = [
        c.lower().strip().replace(" ", "_").replace("-", "_")
        for c in df.columns
    ]

    # Map common alternative column names
    column_aliases = {
        "name": "ward_name",
        "ward": "ward_name",
        "ward_no": "ward_number",
        "pop": "population",
        "total_population": "population",
        "area": "area_sqkm",
        "pop_density": "density",
        "population_density": "density",
        "literacy": "literacy_rate",
        "hh_size": "household_size",
        "avg_household_size": "household_size",
        "households": "total_households",
    }
    df.rename(columns=column_aliases, inplace=True)

    # Validate required columns
    missing = [c for c in EXPECTED_COLUMNS if c not in df.columns]
    if missing:
        # Year can be overridden
        if missing == ["year"] and year_override:
            df["year"] = year_override
        else:
            raise ValueError(
                f"Missing required columns: {missing}. "
                f"Available: {list(df.columns)}"
            )

    # Override year if specified
    if year_override:
        df["year"] = year_override

    # Add optional columns with NaN if missing
    for col in OPTIONAL_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA

    # Cast types
    for col, dtype in COLUMN_TYPES.items():
        if col in df.columns:
            try:
                df[col] = df[col].astype(dtype)
            except (ValueError, TypeError):
                logger.warning(f"Could not cast column '{col}' to {dtype}")

    # Compute density if missing
    if df["density"].isna().all() and "population" in df.columns:
        mask = df["area_sqkm"].notna() & (df["area_sqkm"] > 0)
        df.loc[mask, "density"] = (
            df.loc[mask, "population"] / df.loc[mask, "area_sqkm"]
        )

    # Clean ward names
    df["ward_name"] = df["ward_name"].str.strip().str.title()

    # Remove rows with no ward name or population
    before = len(df)
    df = df.dropna(subset=["ward_name", "population"])
    after = len(df)
    if before != after:
        logger.warning(f"Dropped {before - after} rows with missing data")

    logger.info(f"Loaded {len(df)} census records from {csv_path.name}")
    return df


def validate_census_df(df: pd.DataFrame) -> dict:
    """
    Run validation checks on the census DataFrame.

    Returns:
        Dict with validation results and any warnings
    """
    checks = {
        "total_rows": len(df),
        "unique_wards": df["ward_name"].nunique(),
        "year_range": sorted(df["year"].dropna().unique().tolist()),
        "warnings": [],
    }

    # Check for duplicate wards
    dupes = df.groupby(["ward_name", "year"]).size()
    dupes = dupes[dupes > 1]
    if len(dupes) > 0:
        checks["warnings"].append(
            f"Duplicate ward-year entries: {len(dupes)}"
        )

    # Check population range
    if df["population"].min() < 0:
        checks["warnings"].append("Negative population values found")

    if df["literacy_rate"].max() > 100:
        checks["warnings"].append("Literacy rate > 100% found")

    # Check ward coverage (BBMP has ~198 wards)
    if checks["unique_wards"] < 150:
        checks["warnings"].append(
            f"Low ward coverage: {checks['unique_wards']}/198"
        )

    return checks


def get_prepare_rows(df: pd.DataFrame) -> tuple[list[str], list[tuple]]:
    """
    Prepare census DataFrame for bulk DB insertion into raw.census.

    Returns:
        Tuple of (column_names, row_tuples)
    """
    columns = [
        "ward_name", "ward_number", "population", "male_population",
        "female_population", "area_sqkm", "density", "literacy_rate",
        "male_literacy", "female_literacy", "household_size",
        "total_households", "year"
    ]

    rows = []
    for _, row in df.iterrows():
        rows.append(tuple(
            None if pd.isna(row.get(col)) else row.get(col)
            for col in columns
        ))

    return columns, rows
