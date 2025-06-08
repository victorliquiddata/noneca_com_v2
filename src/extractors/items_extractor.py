#!/usr/bin/env python3
# src/extractors/items_extractor.py
"""Extractor for fetching product catalog data with pagination support."""
import logging
from typing import List, Dict, Optional
from src.extractors.ml_api_client import create_client

logger = logging.getLogger(__name__)


def extract_items(seller_id: str, limit: Optional[int] = None) -> List[Dict]:
    """
    Extract items for a given seller using the ML API client with pagination.

    Args:
        seller_id: The seller ID to extract items for
        limit: Maximum number of items to extract (None for all items, default: None)

    Returns:
        List of item dictionaries, or empty list if extraction fails
    """
    if not seller_id:
        logger.error("Seller ID is required")
        return []

    if limit is not None and limit <= 0:
        logger.error("Limit must be positive or None")
        return []

    try:
        client, token = create_client()

        # Log the extraction attempt
        if limit is None:
            logger.info(f"Starting extraction of ALL items for seller {seller_id}")
        else:
            logger.info(
                f"Starting extraction of up to {limit} items for seller {seller_id}"
            )

        items = client.get_items(token, seller_id, limit=limit)

        if not items:
            logger.info(f"No items found for seller {seller_id}")
            return []

        logger.info(f"Successfully extracted {len(items)} items for seller {seller_id}")
        return items

    except Exception as e:
        logger.error(f"Failed to extract items for seller {seller_id}: {e}")
        return []


def extract_item_details(item_id: str, token: Optional[str] = None) -> Optional[Dict]:
    """
    Extract detailed information for a single item.

    Args:
        item_id: The item ID to extract details for
        token: Authentication token. If None, will create new client.

    Returns:
        Item details dictionary, or None if extraction fails
    """
    if not item_id:
        logger.error("Item ID is required")
        return None

    try:
        if token is None:
            client, token = create_client()
        else:
            client = create_client()[0]

        item_details = client.get_item(token, item_id)

        if item_details:
            logger.info(f"Successfully extracted details for item {item_id}")
            return item_details
        else:
            logger.warning(f"No details found for item {item_id}")
            return None

    except Exception as e:
        logger.error(f"Failed to extract details for item {item_id}: {e}")
        return None


def extract_items_with_enrichments(
    seller_id: str,
    limit: Optional[int] = None,
    include_descriptions: bool = True,
    include_reviews: bool = False,
) -> List[Dict]:
    """
    Extract items with additional details like descriptions and optionally reviews.

    Args:
        seller_id: The seller ID to extract items for
        limit: Maximum number of items to extract (None for all items)
        include_descriptions: Whether to include item descriptions
        include_reviews: Whether to include review data

    Returns:
        List of enriched item dictionaries
    """
    if not seller_id:
        logger.error("Seller ID is required")
        return []

    try:
        client, token = create_client()
        items = extract_items(seller_id, limit)

        if not items:
            return []

        logger.info(f"Starting enrichment of {len(items)} items for seller {seller_id}")
        enriched_items = []

        for i, item in enumerate(items, 1):
            item_id = item.get("id")
            if not item_id:
                enriched_items.append(item)
                continue

            enriched_item = item.copy()

            try:
                # Add description if requested
                if include_descriptions:
                    description = client.get_desc(token, item_id)
                    enriched_item["description"] = description.get("plain_text", "N/A")

                # Add reviews if requested
                if include_reviews:
                    reviews = client.get_reviews(token, item_id)
                    enriched_item["rating_average"] = reviews.get("rating_average", 0)
                    enriched_item["total_reviews"] = reviews.get("total_reviews", 0)

            except Exception as e:
                logger.warning(f"Failed to enrich item {item_id}: {e}")

            enriched_items.append(enriched_item)

            # Log progress every 25 items
            if i % 25 == 0:
                logger.info(f"Enriched {i}/{len(items)} items for seller {seller_id}")

        logger.info(
            f"Successfully enriched {len(enriched_items)} items for seller {seller_id}"
        )
        return enriched_items

    except Exception as e:
        logger.error(f"Failed to extract enriched items for seller {seller_id}: {e}")
        return []
