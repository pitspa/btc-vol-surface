"""
cleanup.py — FIFO Rolling Window Cleanup

Scans the surfaces/ and signals/ directories and deletes any CSV files
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
SIGNALS_DIR = REPO_ROOT / CONFIG.get("signals_dir", "signals")
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


def get_csv_files(directory: Path) -> list:
    """Return all .csv files in the given directory, sorted by name."""
    if not directory.exists():
        return []
    return sorted(directory.glob("*.csv"))


def parse_date_from_filename(filepath: Path):
    """
    Extract a date from a filename like '2026-03-17.csv'.
    Returns None if the filename doesn't match the expected pattern.
    """
    try:
        date_str = filepath.stem
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return None


def cleanup_directory(directory: Path):
    """Delete CSV files older than the rolling window in the given directory."""
    cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=WINDOW_DAYS)
    csv_files = get_csv_files(directory)

    logger.info("Directory: %s | Found %d CSV files", directory, len(csv_files))

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

    logger.info("  Result: %d removed, %d kept.", removed, kept)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    logger.info("=== FIFO Cleanup Started (window: %d days) ===", WINDOW_DAYS)
    cleanup_directory(SURFACES_DIR)
    cleanup_directory(SIGNALS_DIR)
    logger.info("=== Cleanup Complete ===")


if __name__ == "__main__":
    main()
