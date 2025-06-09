#!/usr/bin/env python3
# src/extractors/orders_extractor.py

import logging
from typing import List, Optional, Dict, Any

from src.extractors.ml_api_client import create_client, MLClient

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def extract_orders(
    seller_id: str,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    sort: str = "date_created",
    limit: int = 100,
    max_records: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Extracts orders for a given seller from the ML API using pagination.

    Args:
        seller_id: The seller identifier as a string.
        date_from: ISO-8601 date string for starting filter (inclusive).
        date_to: ISO-8601 date string for ending filter (inclusive).
        sort: Field to sort by (e.g., 'date_created').
        limit: Page size (number of records to fetch per call).
        max_records: Optional cap on total records to retrieve.

    Returns:
        A list of raw order dictionaries as returned by the API.
    """
    client, token = create_client()
    all_orders: List[Dict[str, Any]] = []
    offset = 0

    logger.info(
        "Starting extraction of orders for seller_id=%s from %s to %s",
        seller_id,
        date_from,
        date_to,
    )

    while True:
        # Fetch one “page” of orders
        page = client.get_orders(
            token,
            seller_id,
            date_from=date_from,
            date_to=date_to,
            sort=sort,
            limit=limit,
            offset=offset,
        )

        batch = page.get("results", [])
        if not batch:
            logger.debug("No more orders returned at offset %d", offset)
            break

        all_orders.extend(batch)
        logger.info(
            "Fetched %d orders (offset %d – total so far %d)",
            len(batch),
            offset,
            len(all_orders),
        )

        # Honor max_records if provided
        if max_records and len(all_orders) >= max_records:
            all_orders = all_orders[:max_records]
            logger.info("Reached max_records limit of %d", max_records)
            break

        # Advance offset for next page
        # Use returned paging info if available, else fall back to offset + limit
        paging = page.get("paging", {})
        offset = paging.get("offset", offset) + paging.get("limit", limit)

    logger.info("Completed extraction: total orders fetched = %d", len(all_orders))
    return all_orders


__all__ = ["extract_orders"]
