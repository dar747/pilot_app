# notam/services/fetcher.py (or airport_config.py)
from pathlib import Path
import logging
import pandas as pd

log = logging.getLogger(__name__)


def default_csv_path() -> Path:
    # project root = parent of the 'notam' package
    root = Path(__file__).resolve().parents[2]
    return root / "data" / "Airport Database - NOTAM ID.csv"


def load_monitored_airports(csv_path: str = None) -> set:
    """Load airport codes from CSV Designator column"""
    if csv_path is None:
        csv_path = default_csv_path()
    else:
        csv_path = Path(csv_path)

    try:
        if not csv_path.exists():
            log.error("CSV file not found: %s", csv_path)
            return set()

        df = pd.read_csv(csv_path, usecols=["Designator"])
        df = df.dropna(subset=["Designator"])

        # Clean and extract airport codes
        airport_codes = set()
        for _, row in df.iterrows():
            designator = str(row["Designator"]).strip().upper()
            if designator and designator != "NAN" and len(designator) == 4:
                airport_codes.add(designator)

        log.info("📍 Loaded %d airport codes from %s", len(airport_codes), csv_path.name)
        return airport_codes

    except Exception as e:
        log.error("❌ Failed to load airports from %s: %s", csv_path, e)
        return set()