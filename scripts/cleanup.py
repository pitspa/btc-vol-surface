"""
cleanup.py — FIFO Rolling Window Cleanup

Scans the surfaces/ directory and deletes any CSV files
older than the rolling window defined in config.json.

Usage (from repo root):
    python scripts/cleanup.py
"""

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = REPO_ROOT / "config.json"

with open(CONFIG_PATH, "r") as f:
    CONFIG = json.load(f)

SURFACES_DIR = REPO_ROOT / CONFIG["surfaces_dir"]
WINDOW_DAYS = CONFIG["rolling_window_days"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Cleanup logic
# ---------------------------------------------------------------------------


def get_csv_files() -> list:
    """Return all .csv files in the surfaces directory, sorted by name."""
    if not SURFACES_DIR.exists():
        return []
    return sorted(SURFACES_DIR.glob("*.csv"))


def parse_date_from_filename(filepath: Path) -> datetime | None:
    """
    Extract a date from a filename like '2026-03-17.csv'.
    Returns None if the filename doesn't match the expected pattern.
    """
    try:
        date_str = filepath.stem  # '2026-03-17'
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return None


def cleanup():
    """Delete CSV files older than the rolling window."""
    cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=WINDOW_DAYS)
    csv_files = get_csv_files()

    logger.info("Rolling window: %d days | Cutoff date: %s", WINDOW_DAYS, cutoff.strftime("%Y-%m-%d"))
    logger.info("Found %d CSV files in %s", len(csv_files), SURFACES_DIR)

    removed = 0
    kept = 0

    for filepath in csv_files:
        file_date = parse_date_from_filename(filepath)

        if file_date is None:
            logger.warning("Skipping unrecognised filename: %s", filepath.name)
            continue

        if file_date < cutoff:
            filepath.unlink()
            logger.info("  DELETED: %s (date: %s)", filepath.name, file_date.strftime("%Y-%m-%d"))
            removed += 1
        else:
            kept += 1

    logger.info("Cleanup complete: %d removed, %d kept.", removed, kept)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    logger.info("=== FIFO Cleanup Started ===")
    cleanup()
    logger.info("=== Cleanup Complete ===")


if __name__ == "__main__":
    main()
