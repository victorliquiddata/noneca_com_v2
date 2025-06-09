#!/usr/bin/env python3
# src/transformers/order_enricher.py
"""Applies enrichment logic to order transaction data."""
from datetime import datetime, timezone
from typing import List, Dict, Optional, Any
import pytz


def _parse_ml_datetime(date_str: Optional[str]) -> Optional[datetime]:
    """Parse MercadoLibre datetime string to UTC datetime."""
    if not date_str:
        return None

    try:
        # Handle timezone-aware datetime strings
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc)
    except (ValueError, AttributeError):
        return None


def _normalize_to_sao_paulo(dt: Optional[datetime]) -> Optional[datetime]:
    """Convert datetime to São Paulo timezone."""
    if not dt:
        return None

    sao_paulo_tz = pytz.timezone("America/Sao_Paulo")
    return dt.astimezone(sao_paulo_tz)


def _calculate_profit_margin(total_amount: float, fees: float) -> float:
    """Calculate profit margin percentage after fees."""
    if not total_amount or total_amount <= 0:
        return 0.0

    net_revenue = total_amount - fees
    return round((net_revenue / total_amount) * 100, 2)


def _extract_order_items(order_items: List[Dict]) -> List[Dict]:
    """Extract and normalize order items."""
    if not order_items:
        return []

    items = []
    for item_data in order_items:
        item = item_data.get("item", {})

        # Extract variation attributes
        variations = {}
        for attr in item.get("variation_attributes", []):
            variations[attr.get("name", "").lower()] = attr.get("value_name")

        items.append(
            {
                "item_id": item.get("id"),
                "title": item.get("title"),
                "category_id": item.get("category_id"),
                "variation_id": item.get("variation_id"),
                "condition": item.get("condition"),
                "quantity": item_data.get("quantity", 0),
                "unit_price": float(item_data.get("unit_price", 0)),
                "full_unit_price": float(item_data.get("full_unit_price", 0)),
                "sale_fee": float(item_data.get("sale_fee", 0)),
                "listing_type": item_data.get("listing_type_id"),
                "color": variations.get("cor"),
                "size": variations.get("tamanho"),
                "seller_sku": item.get("seller_sku"),
            }
        )

    return items


def _extract_payment_info(payments: List[Dict]) -> Dict[str, Any]:
    """Extract and aggregate payment information."""
    if not payments:
        return {
            "total_paid": 0.0,
            "payment_method": None,
            "installments": 0,
            "payment_status": None,
            "date_approved": None,
        }

    # For simplicity, take the first payment (most orders have single payment)
    payment = payments[0]

    return {
        "total_paid": float(payment.get("total_paid_amount", 0)),
        "payment_method": payment.get("payment_method_id"),
        "installments": payment.get("installments", 0),
        "payment_status": payment.get("status"),
        "date_approved": _parse_ml_datetime(payment.get("date_approved")),
        "transaction_amount": float(payment.get("transaction_amount", 0)),
        "taxes_amount": float(payment.get("taxes_amount", 0)),
    }


def enrich_order(order: Dict[str, Any]) -> Dict[str, Any]:
    """
    Enrich a single order with computed fields and standardized format.

    Args:
        order: Raw order dictionary from API

    Returns:
        Enriched order dictionary
    """
    if not order:
        return {}

    # Extract basic order info
    order_id = order.get("id")
    total_amount = float(order.get("total_amount", 0))

    # Extract timestamps
    date_created = _parse_ml_datetime(order.get("date_created"))
    date_closed = _parse_ml_datetime(order.get("date_closed"))
    last_updated = _parse_ml_datetime(order.get("last_updated"))

    # Extract participants
    buyer = order.get("buyer", {})
    seller = order.get("seller", {})

    # Extract payment info
    payment_info = _extract_payment_info(order.get("payments", []))

    # Extract order items
    items = _extract_order_items(order.get("order_items", []))

    # Calculate business metrics
    total_fees = sum(item.get("sale_fee", 0) for item in items)
    profit_margin = _calculate_profit_margin(total_amount, total_fees)
    avg_item_price = total_amount / len(items) if items else 0.0
    total_quantity = sum(item.get("quantity", 0) for item in items)

    # Processing timestamp
    processed_at = datetime.now(timezone.utc)

    return {
        # Order identification
        "order_id": order_id,
        "pack_id": order.get("pack_id"),
        "status": order.get("status"),
        "status_detail": order.get("status_detail"),
        # Financial data
        "total_amount": total_amount,
        "paid_amount": float(order.get("paid_amount", 0)),
        "currency_id": order.get("currency_id"),
        "total_fees": total_fees,
        "profit_margin": profit_margin,
        # Payment information
        "payment_method": payment_info["payment_method"],
        "installments": payment_info["installments"],
        "payment_status": payment_info["payment_status"],
        "date_payment_approved": payment_info["date_approved"],
        # Participants
        "buyer_id": buyer.get("id"),
        "buyer_nickname": buyer.get("nickname"),
        "seller_id": seller.get("id"),
        "seller_nickname": seller.get("nickname"),
        # Timestamps (normalized to São Paulo)
        "date_created": _normalize_to_sao_paulo(date_created),
        "date_closed": _normalize_to_sao_paulo(date_closed),
        "last_updated": _normalize_to_sao_paulo(last_updated),
        "processed_at": processed_at,
        # Order metrics
        "total_items": len(items),
        "total_quantity": total_quantity,
        "avg_item_price": round(avg_item_price, 2),
        # Shipping
        "shipping_id": order.get("shipping", {}).get("id"),
        "shipping_cost": order.get("shipping_cost"),
        # Additional metadata
        "tags": order.get("tags", []),
        "context_channel": order.get("context", {}).get("channel"),
        "context_site": order.get("context", {}).get("site"),
        # Order items (normalized)
        "items": items,
    }


def enrich_orders(raw_orders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Enrich a list of orders with computed fields and standardized format.

    Args:
        raw_orders: List of raw order dictionaries from API

    Returns:
        List of enriched order dictionaries
    """
    if not raw_orders:
        return []

    return [enrich_order(order) for order in raw_orders if order]


def enrich_orders_from_json(json_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Enrich orders from JSON file format (with 'orders' key).

    Args:
        json_data: Dictionary with 'orders' key containing list of orders

    Returns:
        List of enriched order dictionaries
    """
    orders = json_data.get("orders", [])
    return enrich_orders(orders)
