#!/usr/bin/env python3
# fetch_orders_cli.py
"""
Interactive CLI to fetch orders from MercadoLibre seller via pagination
and save results to uniquely named JSON files under separate fetch-type folders.

Features:
1. Always saves output with a unique filename into data/orders/json_raw/{fetch_type}
2. Prints to terminal:
   - Basic operations log
   - File header (first order) and footer (last order)
   - Additional summary info

Interface:
- Prompt for seller ID (default: 354140329)
- Menu options:
  1. Last 500 orders, oldest → newest
  2. Last 5000 orders, oldest → newest
  3. All available orders (raw pipeline)
  4. Last 100 orders, newest → oldest
"""

import json
import os
import sys
import uuid
from pathlib import Path
from datetime import datetime, timedelta
import pytz

# Ensure project root for imports
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, PROJECT_ROOT)

from src.extractors.ml_api_client import create_client

# MercadoLibre /orders/search max limit per request
MAX_API_LIMIT = 50


def fetch_orders(
    seller_id,
    total_count=None,
    page_size=MAX_API_LIMIT,
    date_from=None,
    date_to=None,
    sort="date_asc",
):
    """
    Fetch orders via /orders/search with pagination.

    Args:
        seller_id (str): seller ID
        total_count (int|None): number of orders to fetch; None means fetch all until empty
        page_size (int): batch size per API request
        date_from (str): ISO date from
        date_to (str): ISO date to
        sort (str): 'date_asc' or 'date_desc'
    Returns:
        list: list of order dicts
    """
    if page_size > MAX_API_LIMIT:
        page_size = MAX_API_LIMIT

    client, token = create_client()
    client._auth(token)

    collected = []
    offset = 0

    while True:
        # If total_count set, stop when reached
        if total_count is not None and len(collected) >= total_count:
            break
        limit = (
            page_size
            if total_count is None
            else min(page_size, total_count - len(collected))
        )

        params = {
            "seller": seller_id,
            "limit": limit,
            "offset": offset,
            "sort": sort,
            "order.date_created.from": date_from,
            "order.date_created.to": date_to,
        }
        resp = client._req("GET", "/orders/search", params=params)
        batch = resp.get("results", [])
        if not batch:
            break

        collected.extend(batch)
        offset += len(batch)

        # if fewer returned than requested, no more data
        if len(batch) < limit:
            break

    return collected if total_count is None else collected[:total_count]


def default_date_range(months=12):
    tz = pytz.timezone("America/Sao_Paulo")
    now = datetime.now(tz)
    past = now - timedelta(days=30 * months)
    fmt = "%Y-%m-%dT%H:%M:%S.000-03:00"
    return past.strftime(fmt), now.strftime(fmt)


def save_orders(orders, seller_id, fetch_type):
    """
    Save orders to a JSON file with unique name under:
    ./data/orders/json_raw/{fetch_type}/

    Args:
        orders (list): list of order dicts
        seller_id (str): seller ID
        fetch_type (str): one of 'last500', 'last5000', 'all', 'last100'

    Returns:
        str: path to saved file
    """
    # Build directory relative to project root
    data_dir = (
        Path(__file__).resolve().parent.parent
        / "data"
        / "orders"
        / "json_raw"
        / fetch_type
    )
    data_dir.mkdir(parents=True, exist_ok=True)

    # Create a unique filename
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    unique_id = uuid.uuid4().hex[:8]
    filename = f"orders_{seller_id}_{timestamp}_{unique_id}.json"

    # Full file path
    filepath = data_dir / filename

    # Write JSON
    with filepath.open("w") as f:
        json.dump({"orders": orders}, f, indent=2)

    return str(filepath)


def print_summary(orders, filename):
    count = len(orders)
    if count == 0:
        print("No orders fetched.")
        return

    first = orders[0]["date_created"]
    last = orders[-1]["date_created"]

    print(f"\nSaved {count} orders to {filename}")
    print(f"Date range in file: {first} → {last}")
    print("Sample record (first):", json.dumps(orders[0], indent=2))
    if count > 1:
        print("Sample record (last):", json.dumps(orders[-1], indent=2))


def main():
    seller = input("Enter seller ID [354140329]: ").strip() or "354140329"

    print("\nSelect retrieval option:")
    print(" 1) Last 500 orders, oldest → newest")
    print(" 2) Last 5000 orders, oldest → newest")
    print(" 3) All available orders (last 12 months)")
    print(" 4) Last 100 orders, newest → oldest")
    choice = input("Choice [1-4]: ").strip()

    date_from, date_to = default_date_range(months=12)

    # Map choice to fetch parameters and type
    fetch_type_map = {
        "1": (500, "date_asc", "last500"),
        "2": (5000, "date_asc", "last5000"),
        "3": (None, "date_asc", "all"),
        "4": (100, "date_desc", "last100"),
    }

    if choice not in fetch_type_map:
        print("Invalid choice, exiting.")
        sys.exit(1)

    total_count, sort_order, fetch_type = fetch_type_map[choice]
    orders = fetch_orders(
        seller,
        total_count=total_count,
        page_size=MAX_API_LIMIT,
        date_from=date_from,
        date_to=date_to,
        sort=sort_order,
    )

    fname = save_orders(orders, seller, fetch_type)
    print_summary(orders, fname)


if __name__ == "__main__":
    main()
