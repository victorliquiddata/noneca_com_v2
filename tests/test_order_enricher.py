# tests/test_order_enricher.py
"""Tests for order enrichment functionality."""
import pytest
from datetime import datetime, timezone
import pytz
from src.transformers.order_enricher import (
    enrich_order,
    enrich_orders,
    enrich_orders_from_json,
    _parse_ml_datetime,
    _normalize_to_sao_paulo,
    _calculate_profit_margin,
    _extract_order_items,
    _extract_payment_info,
)


@pytest.fixture
def sample_order():
    """Sample order data based on the provided JSON structure."""
    return {
        "id": 2000011882898554,
        "status": "paid",
        "status_detail": None,
        "total_amount": 237.12,
        "paid_amount": 237.12,
        "currency_id": "BRL",
        "date_created": "2025-06-08T16:19:34.000-04:00",
        "date_closed": "2025-06-08T16:19:37.000-04:00",
        "last_updated": "2025-06-08T16:21:29.000-04:00",
        "pack_id": 2000008124565315,
        "shipping_cost": None,
        "buyer": {"id": 2314952791, "nickname": "SA20250309214042"},
        "seller": {"id": 354140329, "nickname": "NONECA_CALCINHAS_TRANS"},
        "payments": [
            {
                "id": 113924467653,
                "status": "approved",
                "status_detail": "accredited",
                "total_paid_amount": 237.12,
                "transaction_amount": 237.12,
                "payment_method_id": "account_money",
                "installments": 1,
                "date_approved": "2025-06-08T16:19:36.000-04:00",
                "taxes_amount": 0,
            }
        ],
        "order_items": [
            {
                "item": {
                    "id": "MLB3924389013",
                    "title": "Noneca - Calcinha Aquendar Slim - Marquinha - Bronzeado",
                    "category_id": "MLB4954",
                    "variation_id": 182392519166,
                    "condition": "new",
                    "variation_attributes": [
                        {
                            "name": "Cor",
                            "id": "COLOR",
                            "value_id": "52043",
                            "value_name": "Rosa-claro",
                        },
                        {
                            "name": "Tamanho",
                            "id": "SIZE",
                            "value_id": "433336",
                            "value_name": "GG",
                        },
                    ],
                    "seller_sku": "MLB3924389013_182392519166",
                },
                "quantity": 6,
                "unit_price": 39.52,
                "full_unit_price": 41.6,
                "sale_fee": 14.01,
                "listing_type_id": "gold_pro",
            }
        ],
        "shipping": {"id": 44984392788},
        "tags": ["pack_order", "order_has_discount", "paid", "not_delivered"],
        "context": {"channel": "marketplace", "site": "MLB"},
    }


@pytest.fixture
def sample_json_data(sample_order):
    """Sample JSON data with orders array."""
    return {"orders": [sample_order]}


class TestDatetimeUtilities:
    """Test datetime parsing and conversion utilities."""

    def test_parse_ml_datetime_valid(self):
        """Test parsing valid MercadoLibre datetime."""
        date_str = "2025-06-08T16:19:34.000-04:00"
        result = _parse_ml_datetime(date_str)

        assert result is not None
        assert isinstance(result, datetime)
        assert result.tzinfo is not None

    def test_parse_ml_datetime_none(self):
        """Test parsing None datetime."""
        assert _parse_ml_datetime(None) is None

    def test_parse_ml_datetime_invalid(self):
        """Test parsing invalid datetime."""
        assert _parse_ml_datetime("invalid-date") is None

    def test_normalize_to_sao_paulo(self):
        """Test timezone normalization to São Paulo."""
        utc_dt = datetime(2025, 6, 8, 20, 19, 34, tzinfo=timezone.utc)
        result = _normalize_to_sao_paulo(utc_dt)

        assert result is not None
        assert result.tzinfo.zone == "America/Sao_Paulo"

    def test_normalize_to_sao_paulo_none(self):
        """Test normalizing None datetime."""
        assert _normalize_to_sao_paulo(None) is None


class TestBusinessMetrics:
    """Test business metric calculations."""

    def test_calculate_profit_margin_positive(self):
        """Test profit margin calculation with positive values."""
        margin = _calculate_profit_margin(100.0, 15.0)
        assert margin == 85.0

    def test_calculate_profit_margin_zero_total(self):
        """Test profit margin with zero total amount."""
        margin = _calculate_profit_margin(0.0, 15.0)
        assert margin == 0.0

    def test_calculate_profit_margin_high_fees(self):
        """Test profit margin with fees higher than total."""
        margin = _calculate_profit_margin(100.0, 120.0)
        assert margin == -20.0


class TestOrderItemExtraction:
    """Test order item extraction and normalization."""

    def test_extract_order_items_valid(self, sample_order):
        """Test extracting valid order items."""
        items = _extract_order_items(sample_order["order_items"])

        assert len(items) == 1
        item = items[0]

        assert item["item_id"] == "MLB3924389013"
        assert (
            item["title"] == "Noneca - Calcinha Aquendar Slim - Marquinha - Bronzeado"
        )
        assert item["quantity"] == 6
        assert item["unit_price"] == 39.52
        assert item["color"] == "Rosa-claro"
        assert item["size"] == "GG"

    def test_extract_order_items_empty(self):
        """Test extracting from empty order items."""
        items = _extract_order_items([])
        assert items == []

    def test_extract_order_items_none(self):
        """Test extracting from None order items."""
        items = _extract_order_items(None)
        assert items == []


class TestPaymentExtraction:
    """Test payment information extraction."""

    def test_extract_payment_info_valid(self, sample_order):
        """Test extracting valid payment info."""
        payment_info = _extract_payment_info(sample_order["payments"])

        assert payment_info["total_paid"] == 237.12
        assert payment_info["payment_method"] == "account_money"
        assert payment_info["installments"] == 1
        assert payment_info["payment_status"] == "approved"
        assert payment_info["date_approved"] is not None

    def test_extract_payment_info_empty(self):
        """Test extracting from empty payments."""
        payment_info = _extract_payment_info([])

        assert payment_info["total_paid"] == 0.0
        assert payment_info["payment_method"] is None
        assert payment_info["installments"] == 0
        assert payment_info["payment_status"] is None
        assert payment_info["date_approved"] is None


class TestOrderEnrichment:
    """Test complete order enrichment."""

    def test_enrich_order_complete(self, sample_order):
        """Test enriching a complete order."""
        enriched = enrich_order(sample_order)

        # Test basic fields
        assert enriched["order_id"] == 2000011882898554
        assert enriched["status"] == "paid"
        assert enriched["total_amount"] == 237.12
        assert enriched["currency_id"] == "BRL"

        # Test participant info
        assert enriched["buyer_id"] == 2314952791
        assert enriched["seller_id"] == 354140329

        # Test payment info
        assert enriched["payment_method"] == "account_money"
        assert enriched["installments"] == 1

        # Test computed metrics
        assert enriched["total_items"] == 1
        assert enriched["total_quantity"] == 6
        assert enriched["avg_item_price"] == 237.12
        assert enriched["total_fees"] == 14.01

        # Test datetime fields
        assert enriched["date_created"] is not None
        assert enriched["processed_at"] is not None

        # Test nested items
        assert len(enriched["items"]) == 1
        assert enriched["items"][0]["item_id"] == "MLB3924389013"

    def test_enrich_order_empty(self):
        """Test enriching empty order."""
        enriched = enrich_order({})
        assert enriched == {}

    def test_enrich_order_none(self):
        """Test enriching None order."""
        enriched = enrich_order(None)
        assert enriched == {}

    def test_enrich_order_minimal(self):
        """Test enriching order with minimal data."""
        minimal_order = {"id": 123456, "status": "pending", "total_amount": 50.0}

        enriched = enrich_order(minimal_order)

        assert enriched["order_id"] == 123456
        assert enriched["status"] == "pending"
        assert enriched["total_amount"] == 50.0
        assert enriched["total_items"] == 0
        assert enriched["items"] == []


class TestBatchEnrichment:
    """Test batch order enrichment."""

    def test_enrich_orders_list(self, sample_order):
        """Test enriching list of orders."""
        orders = [sample_order, sample_order.copy()]
        enriched = enrich_orders(orders)

        assert len(enriched) == 2
        assert all("order_id" in order for order in enriched)

    def test_enrich_orders_empty(self):
        """Test enriching empty list."""
        enriched = enrich_orders([])
        assert enriched == []

    def test_enrich_orders_none(self):
        """Test enriching None."""
        enriched = enrich_orders(None)
        assert enriched == []

    def test_enrich_orders_from_json(self, sample_json_data):
        """Test enriching orders from JSON format."""
        enriched = enrich_orders_from_json(sample_json_data)

        assert len(enriched) == 1
        assert enriched[0]["order_id"] == 2000011882898554

    def test_enrich_orders_from_json_empty(self):
        """Test enriching from empty JSON."""
        enriched = enrich_orders_from_json({"orders": []})
        assert enriched == []

    def test_enrich_orders_from_json_no_orders_key(self):
        """Test enriching JSON without orders key."""
        enriched = enrich_orders_from_json({"data": []})
        assert enriched == []


class TestIntegration:
    """Integration tests for complete workflow."""

    def test_full_workflow(self, sample_json_data):
        """Test complete enrichment workflow."""
        # Simulate reading JSON file output from fetch_orders_cli.py
        enriched_orders = enrich_orders_from_json(sample_json_data)

        assert len(enriched_orders) == 1
        order = enriched_orders[0]

        # Verify all expected fields are present
        required_fields = [
            "order_id",
            "status",
            "total_amount",
            "buyer_id",
            "seller_id",
            "date_created",
            "payment_method",
            "total_items",
            "items",
        ]

        for field in required_fields:
            assert field in order, f"Missing required field: {field}"

        # Verify business logic
        assert order["profit_margin"] > 0  # Should have positive margin
        assert order["total_quantity"] > 0  # Should have items
        assert len(order["items"]) > 0  # Should have item details
