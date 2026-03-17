"""
collect.py — Daily BTC Volatility Surface Snapshot Collector

Fetches the full BTC European option chain from the Deribit public API
and saves it as a dated CSV in the surfaces/ directory.

Usage (from repo root):
    python scripts/collect.py

No API key required. Uses only public endpoints.

API endpoints used:
    1. public/get_instruments   — lists all active BTC option contracts
    2. public/get_order_book    — returns bid/ask, mark price, IV, greeks per instrument
    3. public/get_index_price   — returns the current BTC spot index price
"""

import sys
import json
import time
import logging
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import pandas as pd

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = REPO_ROOT / "config.json"

with open(CONFIG_PATH, "r") as f:
    CONFIG = json.load(f)

BASE_URL = CONFIG["deribit_base_url"]
CURRENCY = CONFIG["currency"]
SURFACES_DIR = REPO_ROOT / CONFIG["surfaces_dir"]
MIN_OI = CONFIG.get("min_open_interest", 0)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Deribit API helpers
# ---------------------------------------------------------------------------

SESSION = requests.Session()
SESSION.headers.update({"Content-Type": "application/json"})

# Conservative delay between individual order-book requests
REQUEST_DELAY = 0.12  # seconds


def api_get(method: str, params: dict) -> dict:
    """Call a Deribit public API v2 method via HTTP GET."""
    url = f"{BASE_URL}/{method}"
    try:
        resp = SESSION.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if "result" not in data:
            raise ValueError(f"No 'result' in response: {data}")
        return data["result"]
    except requests.exceptions.RequestException as exc:
        logger.error("API request failed for %s: %s", method, exc)
        raise


def get_active_option_instruments() -> list:
    """Return all active BTC option instruments from Deribit."""
    instruments = api_get("get_instruments", {
        "currency": CURRENCY,
        "kind": "option",
        "expired": "false",
    })
    logger.info("Found %d active %s option instruments.", len(instruments), CURRENCY)
    return instruments


def get_index_price() -> float:
    """
    Fetch the current BTC spot index price from Deribit.
    This is the composite spot price (not a forward), computed from
    multiple exchanges. It is the same index used for settlement.
    Raises on failure — never returns fallback data.
    """
    result = api_get("get_index_price", {
        "index_name": f"btc_usd",
    })
    price = result.get("index_price")
    if price is None:
        raise ValueError(f"No 'index_price' in response: {result}")
    logger.info("BTC spot index price: %.2f USD", price)
    return float(price)


MAX_RETRIES = 3
RETRY_BACKOFF = 2.0  # seconds, doubles each retry


def get_order_book(instrument_name: str) -> dict:
    """
    Fetch the order book for a single instrument.
    Retries up to MAX_RETRIES times on failure.
    Raises on final failure — never returns fallback data.
    """
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            time.sleep(REQUEST_DELAY)
            result = api_get("get_order_book", {
                "instrument_name": instrument_name,
            })
            return result
        except Exception as exc:
            last_exc = exc
            if attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF * (2 ** (attempt - 1))
                logger.warning(
                    "Attempt %d/%d failed for %s: %s — retrying in %.1fs",
                    attempt, MAX_RETRIES, instrument_name, exc, wait,
                )
                time.sleep(wait)
            else:
                logger.error(
                    "All %d attempts failed for %s: %s",
                    MAX_RETRIES, instrument_name, last_exc,
                )
                raise last_exc


# ---------------------------------------------------------------------------
# Instrument name parser
# ---------------------------------------------------------------------------

def parse_instrument_name(name: str) -> dict:
    """
    Parse a Deribit option instrument name like 'BTC-28MAR26-90000-C'.
    Returns dict with expiry_str, strike, option_type.
    """
    parts = name.split("-")
    return {
        "expiry_str": parts[1],
        "strike": float(parts[2]),
        "option_type": parts[3],
    }


def deribit_expiry_to_date(expiry_str: str) -> str:
    """Convert Deribit expiry string '28MAR26' to 'YYYY-MM-DD'."""
    return datetime.strptime(expiry_str, "%d%b%y").strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# Main collection logic
# ---------------------------------------------------------------------------

def collect_snapshot() -> pd.DataFrame:
    """
    Collect the full BTC option surface and return as a DataFrame.

    Strategy:
        1. Get all active option instrument names.
        2. For each instrument, call get_order_book (threaded, 5 workers).
        3. Parse and assemble into a flat DataFrame.
    """
    snapshot_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    instruments = get_active_option_instruments()
    instrument_names = [inst["instrument_name"] for inst in instruments]

    # Fetch the BTC spot index price (single value, same for all rows)
    index_price = get_index_price()

    logger.info("Fetching order books for %d instruments (this may take a few minutes)...",
                len(instrument_names))

    # Threaded fetch — keep concurrency low for unauthenticated access
    # STRICT: every failure is tracked. If too many instruments fail,
    # the entire snapshot is aborted. No partial saves, no fallback data.
    FAILURE_THRESHOLD = 0.05  # abort if more than 5% of instruments fail

    order_books = {}
    failed_instruments = []

    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_name = {
            executor.submit(get_order_book, name): name
            for name in instrument_names
        }
        done_count = 0
        for future in as_completed(future_to_name):
            name = future_to_name[future]
            try:
                result = future.result()
                order_books[name] = result
            except Exception as exc:
                failed_instruments.append(name)
                logger.error("FAILED (after retries): %s — %s", name, exc)
            done_count += 1
            if done_count % 100 == 0:
                logger.info("  ... processed %d / %d", done_count, len(instrument_names))

    n_total = len(instrument_names)
    n_failed = len(failed_instruments)
    n_ok = len(order_books)
    failure_rate = n_failed / n_total if n_total > 0 else 0.0

    logger.info("Fetch results: %d OK, %d FAILED out of %d total (%.1f%% failure rate).",
                n_ok, n_failed, n_total, failure_rate * 100)

    if n_failed > 0:
        logger.error("Failed instruments: %s", failed_instruments)

    if failure_rate > FAILURE_THRESHOLD:
        raise RuntimeError(
            f"Aborting: failure rate {failure_rate:.1%} exceeds threshold "
            f"{FAILURE_THRESHOLD:.0%}. {n_failed}/{n_total} instruments failed. "
            f"No snapshot saved."
        )

    # Assemble rows
    rows = []
    for name, ob in order_books.items():
        parsed = parse_instrument_name(name)

        try:
            expiry_date = deribit_expiry_to_date(parsed["expiry_str"])
        except ValueError:
            logger.warning("Could not parse expiry for %s, skipping.", name)
            continue

        expiry_dt = datetime.strptime(expiry_date, "%Y-%m-%d")
        now_date = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0, tzinfo=None
        )
        days_to_expiry = (expiry_dt - now_date).days

        greeks = ob.get("greeks", {}) or {}
        stats = ob.get("stats", {}) or {}

        rows.append({
            "snapshot_ts": snapshot_ts,
            "expiry": expiry_date,
            "days_to_expiry": days_to_expiry,
            "strike": parsed["strike"],
            "option_type": parsed["option_type"],
            "bid": ob.get("best_bid_price"),
            "ask": ob.get("best_ask_price"),
            "mark_price": ob.get("mark_price"),
            "forward_price": ob.get("underlying_price"),
            "index_price": index_price,
            "implied_vol": ob.get("mark_iv"),
            "delta": greeks.get("delta"),
            "gamma": greeks.get("gamma"),
            "vega": greeks.get("vega"),
            "open_interest": ob.get("open_interest"),
            "volume_24h": stats.get("volume"),
        })

    df = pd.DataFrame(rows)

    if df.empty:
        logger.warning("No data collected. DataFrame is empty.")
        return df

    # Convert mark_iv from percentage (65.0) to decimal (0.65)
    if "implied_vol" in df.columns:
        df["implied_vol"] = pd.to_numeric(df["implied_vol"], errors="coerce") / 100.0

    # Sort: expiry → strike → option type
    df = df.sort_values(
        by=["expiry", "strike", "option_type"],
        ascending=[True, True, True],
    ).reset_index(drop=True)

    # Optionally filter by minimum open interest
    if MIN_OI > 0:
        before = len(df)
        df = df[df["open_interest"] >= MIN_OI].reset_index(drop=True)
        logger.info("Filtered by min OI=%s: %d -> %d rows.", MIN_OI, before, len(df))

    return df


def save_snapshot(df: pd.DataFrame) -> Path:
    """Save the DataFrame as a dated CSV in the surfaces directory."""
    SURFACES_DIR.mkdir(parents=True, exist_ok=True)

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    filepath = SURFACES_DIR / f"{today}.csv"

    df.to_csv(filepath, index=False)
    size_kb = filepath.stat().st_size / 1024
    logger.info("Saved snapshot: %s (%d rows, %.1f KB)", filepath.name, len(df), size_kb)

    return filepath


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    logger.info("=== BTC Vol Surface Collection Started ===")
    logger.info("Currency: %s | Surfaces dir: %s", CURRENCY, SURFACES_DIR)

    try:
        df = collect_snapshot()
    except Exception as exc:
        logger.error("Collection failed: %s", exc)
        sys.exit(1)

    if df.empty:
        logger.error("No data collected. Exiting with error.")
        sys.exit(1)

    save_snapshot(df)
    logger.info("=== Collection Complete ===")


if __name__ == "__main__":
    main()
