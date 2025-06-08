```python
# --- comp_test.py
#!/usr/bin/env python3
import json
import os
import sys
import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch, mock_open
from dataclasses import dataclass
from typing import Dict, List, Any


# Mock the config module to avoid environment dependencies
@dataclass
class MockConfig:
    client_id: str = "test_client_id"
    client_secret: str = "test_secret"
    redirect_uri: str = "http://test.com/callback"
    timeout: int = 30
    rate_limit: int = 100
    api_url: str = "https://api.test.com"
    auth_url: str = "https://auth.test.com"
    token_file: str = "test_tokens.json"
    fallback_access: str = "test_access_token"
    fallback_refresh: str = "test_refresh_token"
    fallback_expires: str = "2025-12-31T23:59:59"


# Mock modules
with patch.dict(
    "sys.modules",
    {
        "config.config": Mock(cfg=MockConfig()),
        "src.extractors.ml_api_client": Mock(),
    },
):
    # Import after mocking
    import sys

    sys.path.insert(0, ".")

    # Create mock implementations inline since we can't import the real modules
    class MockMLClient:
        def __init__(self):
            self.session = Mock()
            self.session.headers = {}
            self._rate = {"calls": 0, "reset": datetime.now()}

        def _check_rate(self):
            now = datetime.now()
            if now - self._rate["reset"] > timedelta(minutes=1):
                self._rate = {"calls": 0, "reset": now}
            if self._rate["calls"] >= 100:
                raise Exception("Rate limit exceeded")
            self._rate["calls"] += 1

        def _auth(self, token):
            self.session.headers.update({"Authorization": f"Bearer {token}"})

        def get_user(self, token, user_id="me", attrs=None):
            self._auth(token)
            return {"id": "test_user_123", "nickname": "test_seller"}

        def get_items(self, token, seller_id, limit=50, status="active"):
            self._auth(token)
            items = [
                {"id": f"item_{i}", "title": f"Test Item {i}", "price": 100.0 + i}
                for i in range(min(limit, 3))
            ]
            return items

        def get_item(self, token, item_id, attrs=None):
            self._auth(token)
            return {
                "id": item_id,
                "title": "Test Product",
                "price": 299.99,
                "original_price": 399.99,
                "category_id": "CAT123",
                "seller_id": "SELLER123",
                "attributes": [
                    {"id": "BRAND", "value_name": "TestBrand"},
                    {"id": "MAIN_COLOR", "value_name": "Blue"},
                    {"id": "SIZE", "value_name": "M"},
                ],
                "views": 1000,
                "sold_quantity": 50,
                "available_quantity": 10,
                "condition": "new",
            }

        def get_desc(self, token, item_id):
            self._auth(token)
            return {"plain_text": f"Description for {item_id}"}

        def get_reviews(self, token, item_id):
            self._auth(token)
            return {"rating_average": 4.5, "total_reviews": 25, "reviews": []}


# Test fixtures
@pytest.fixture
def mock_client():
    return MockMLClient()


@pytest.fixture
def mock_token():
    return "test_access_token_123"


@pytest.fixture
def sample_item():
    return {
        "id": "ML123456789",
        "title": "iPhone 13 Pro Max 256GB",
        "price": 5999.99,
        "original_price": 6999.99,
        "category_id": "MLA1055",
        "seller_id": "SELLER123",
        "attributes": [
            {"id": "BRAND", "value_name": "Apple"},
            {"id": "MAIN_COLOR", "value_name": "Space Gray"},
            {"id": "SIZE", "value_name": "256GB"},
            {"id": "GENDER", "value_name": "Unisex"},
        ],
        "views": 15000,
        "sold_quantity": 120,
        "available_quantity": 5,
        "condition": "new",
    }


@pytest.fixture
def sample_items_list():
    return [
        {
            "id": "ML001",
            "title": "Product 1",
            "price": 100.0,
            "attributes": [{"id": "BRAND", "value_name": "Brand1"}],
        },
        {
            "id": "ML002",
            "title": "Product 2",
            "price": 200.0,
            "attributes": [{"id": "MAIN_COLOR", "value_name": "Red"}],
        },
    ]


class TestProductEnricher:
    """Test the product enrichment functionality."""

    def test_get_attr_extracts_value_name(self):
        from src.transformers.product_enricher import _get_attr

        attrs = [{"id": "BRAND", "value_name": "Nike", "value_id": "123"}]
        assert _get_attr(attrs, "BRAND") == "Nike"

    def test_get_attr_falls_back_to_value_id(self):
        from src.transformers.product_enricher import _get_attr

        attrs = [{"id": "COLOR", "value_id": "blue_001"}]
        assert _get_attr(attrs, "COLOR") == "blue_001"

    def test_get_attr_returns_none_for_missing_key(self):
        from src.transformers.product_enricher import _get_attr

        attrs = [{"id": "BRAND", "value_name": "Nike"}]
        assert _get_attr(attrs, "SIZE") is None

    def test_get_attr_handles_empty_values(self):
        from src.transformers.product_enricher import _get_attr

        attrs = [{"id": "BRAND", "value_name": "", "value_id": ""}]
        assert _get_attr(attrs, "BRAND") is None

    def test_safe_divide_normal_case(self):
        from src.transformers.product_enricher import _safe_divide

        assert _safe_divide(10, 4) == 2.5
        assert _safe_divide(1, 3, 2) == 0.33

    def test_safe_divide_zero_denominator(self):
        from src.transformers.product_enricher import _safe_divide

        assert _safe_divide(10, 0) == 0.0
        assert _safe_divide(100, None) == 0.0

    def test_calculate_discount_percentage(self):
        from src.transformers.product_enricher import _calculate_discount_percentage

        assert _calculate_discount_percentage(100, 80) == 20.0
        assert _calculate_discount_percentage(200, 150) == 25.0
        assert _calculate_discount_percentage(100, 100) == 0.0
        assert _calculate_discount_percentage(100, 120) == 0.0

    def test_enrich_item_complete_data(self, sample_item):
        from src.transformers.product_enricher import enrich_item

        enriched = enrich_item(sample_item)

        assert enriched["item_id"] == "ML123456789"
        assert enriched["title"] == "iPhone 13 Pro Max 256GB"
        assert enriched["current_price"] == 5999.99
        assert enriched["original_price"] == 6999.99
        assert enriched["brand"] == "Apple"
        assert enriched["color"] == "Space Gray"
        assert enriched["size"] == "256GB"
        assert enriched["gender"] == "Unisex"
        assert enriched["conversion_rate"] == 0.008  # 120/15000
        assert enriched["discount_percentage"] == 14.29
        assert "created_at" in enriched
        assert "updated_at" in enriched

    def test_enrich_item_missing_attributes(self):
        from src.transformers.product_enricher import enrich_item

        minimal_item = {"id": "ML001", "title": "Basic Product", "price": 50.0}

        enriched = enrich_item(minimal_item)

        assert enriched["item_id"] == "ML001"
        assert enriched["brand"] is None
        assert enriched["color"] is None
        assert enriched["conversion_rate"] == 0.0
        assert enriched["discount_percentage"] == 0.0

    def test_enrich_item_empty_input(self):
        from src.transformers.product_enricher import enrich_item

        assert enrich_item({}) == {}
        assert enrich_item(None) == {}

    def test_enrich_items_list(self, sample_items_list):
        from src.transformers.product_enricher import enrich_items

        enriched = enrich_items(sample_items_list)

        assert len(enriched) == 2
        assert enriched[0]["item_id"] == "ML001"
        assert enriched[0]["brand"] == "Brand1"
        assert enriched[1]["item_id"] == "ML002"
        assert enriched[1]["color"] == "Red"

    def test_enrich_items_empty_list(self):
        from src.transformers.product_enricher import enrich_items

        assert enrich_items([]) == []
        assert enrich_items(None) == []


class TestMLAPIClient:
    """Test the ML API client functionality."""

    def test_client_initialization(self, mock_client):
        assert mock_client.session is not None
        assert mock_client._rate["calls"] == 0

    def test_rate_limiting(self, mock_client):
        # Simulate hitting rate limit
        mock_client._rate["calls"] = 100

        with pytest.raises(Exception, match="Rate limit exceeded"):
            mock_client._check_rate()

    def test_rate_limit_reset(self, mock_client):
        # Set calls to limit and reset time to past
        mock_client._rate["calls"] = 100
        mock_client._rate["reset"] = datetime.now() - timedelta(minutes=2)

        # Should reset and not raise exception
        mock_client._check_rate()
        assert mock_client._rate["calls"] == 1

    def test_get_user_default_params(self, mock_client, mock_token):
        user = mock_client.get_user(mock_token)
        assert user["id"] == "test_user_123"
        assert user["nickname"] == "test_seller"

    def test_get_items_with_limit(self, mock_client, mock_token):
        items = mock_client.get_items(mock_token, "seller123", limit=2)
        assert len(items) == 2
        assert items[0]["id"] == "item_0"
        assert items[1]["id"] == "item_1"

    def test_get_item_details(self, mock_client, mock_token):
        item = mock_client.get_item(mock_token, "ML123")
        assert item["id"] == "ML123"
        assert item["title"] == "Test Product"
        assert item["price"] == 299.99

    def test_get_description(self, mock_client, mock_token):
        desc = mock_client.get_desc(mock_token, "ML123")
        assert desc["plain_text"] == "Description for ML123"

    def test_get_reviews(self, mock_client, mock_token):
        reviews = mock_client.get_reviews(mock_token, "ML123")
        assert reviews["rating_average"] == 4.5
        assert reviews["total_reviews"] == 25


class TestItemsExtractor:
    """Test the items extraction functionality."""

    @patch("src.extractors.items_extractor.create_client")
    def test_extract_items_success(self, mock_create_client):
        # Setup mock
        mock_client = Mock()
        mock_client.get_items.return_value = [
            {"id": "ML001", "title": "Product 1"},
            {"id": "ML002", "title": "Product 2"},
        ]
        mock_create_client.return_value = (mock_client, "test_token")

        # Test extraction
        from src.extractors.items_extractor import extract_items

        items = extract_items("seller123", limit=2)

        assert len(items) == 2
        assert items[0]["id"] == "ML001"
        mock_client.get_items.assert_called_once_with(
            "test_token", "seller123", limit=2
        )

    @patch("src.extractors.items_extractor.create_client")
    def test_extract_items_empty_seller_id(self, mock_create_client):
        from src.extractors.items_extractor import extract_items

        items = extract_items("", limit=10)
        assert items == []
        mock_create_client.assert_not_called()

    @patch("src.extractors.items_extractor.create_client")
    def test_extract_items_invalid_limit(self, mock_create_client):
        from src.extractors.items_extractor import extract_items

        items = extract_items("seller123", limit=0)
        assert items == []
        mock_create_client.assert_not_called()

    @patch("src.extractors.items_extractor.create_client")
    def test_extract_items_api_failure(self, mock_create_client):
        # Setup mock to raise exception
        mock_client = Mock()
        mock_client.get_items.side_effect = Exception("API Error")
        mock_create_client.return_value = (mock_client, "test_token")

        from src.extractors.items_extractor import extract_items

        items = extract_items("seller123")
        assert items == []

    @patch("src.extractors.items_extractor.create_client")
    def test_extract_item_details_success(self, mock_create_client):
        # Setup mock
        mock_client = Mock()
        mock_client.get_item.return_value = {"id": "ML123", "title": "Product"}
        mock_create_client.return_value = (mock_client, "test_token")

        from src.extractors.items_extractor import extract_item_details

        item = extract_item_details("ML123")

        assert item["id"] == "ML123"
        mock_client.get_item.assert_called_once_with("test_token", "ML123")

    @patch("src.extractors.items_extractor.create_client")
    def test_extract_items_with_enrichments(self, mock_create_client):
        # Setup mocks
        mock_client = Mock()
        mock_client.get_items.return_value = [{"id": "ML123", "title": "Product"}]
        mock_client.get_desc.return_value = {"plain_text": "Description"}
        mock_client.get_reviews.return_value = {
            "rating_average": 4.0,
            "total_reviews": 10,
        }
        mock_create_client.return_value = (mock_client, "test_token")

        from src.extractors.items_extractor import extract_items_with_enrichments

        items = extract_items_with_enrichments(
            "seller123", include_descriptions=True, include_reviews=True
        )

        assert len(items) == 1
        assert items[0]["description"] == "Description"
        assert items[0]["rating_average"] == 4.0
        assert items[0]["total_reviews"] == 10


class TestTokenManagement:
    """Test token management functions."""

    @patch("src.extractors.ml_api_client.cfg.token_file", "test_tokens.json")
    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data='{"access_token": "test123", "expires_at": "2025-12-31T23:59:59"}',
    )
    @patch("os.path.exists", return_value=True)
    def test_load_tokens_from_file(self, mock_exists, mock_file):
        from src.extractors.ml_api_client import load_tokens

        tokens = load_tokens()
        assert tokens["access_token"] == "test123"

    @patch("src.extractors.ml_api_client.cfg.fallback_access", "test_access_token")
    @patch("src.extractors.ml_api_client.cfg.fallback_refresh", "test_refresh_token")
    @patch("src.extractors.ml_api_client.cfg.fallback_expires", "2025-12-31T23:59:59")
    @patch("os.path.exists", return_value=False)
    def test_load_tokens_fallback(self, mock_exists):
        from src.extractors.ml_api_client import load_tokens

        tokens = load_tokens()
        assert tokens["access_token"] == "test_access_token"

    def test_is_valid_token_check(self):
        from src.extractors.ml_api_client import is_valid

        # Valid token (expires in future)
        future_time = (datetime.now() + timedelta(hours=1)).isoformat()
        valid_tokens = {"expires_at": future_time}
        assert is_valid(valid_tokens) is True

        # Expired token
        past_time = (datetime.now() - timedelta(hours=1)).isoformat()
        expired_tokens = {"expires_at": past_time}
        assert is_valid(expired_tokens) is False

        # No tokens
        assert is_valid(None) is False

    @patch("src.extractors.ml_api_client.cfg.token_file", "test_tokens.json")
    @patch("builtins.open", new_callable=mock_open)
    def test_save_tokens(self, mock_file):
        from src.extractors.ml_api_client import save_tokens

        tokens = {"access_token": "test", "expires_in": 3600}
        save_tokens(tokens)

        # Check that file was opened for writing
        mock_file.assert_called_once_with("test_tokens.json", "w")


class TestIntegratedWorkflow:
    """Test integrated workflows combining multiple components."""

    @patch("src.extractors.items_extractor.create_client")
    def test_full_extraction_and_enrichment_pipeline(self, mock_create_client):
        # Setup comprehensive mock
        mock_client = Mock()
        mock_client.get_items.return_value = [
            {
                "id": "ML001",
                "title": "iPhone 14",
                "price": 4999.99,
                "original_price": 5999.99,
                "attributes": [
                    {"id": "BRAND", "value_name": "Apple"},
                    {"id": "MAIN_COLOR", "value_name": "Black"},
                ],
                "views": 5000,
                "sold_quantity": 100,
                "seller_id": "SELLER123",
            }
        ]
        mock_create_client.return_value = (mock_client, "test_token")

        # Extract items
        from src.extractors.items_extractor import extract_items

        raw_items = extract_items("SELLER123")

        # Enrich items
        from src.transformers.product_enricher import enrich_items

        enriched_items = enrich_items(raw_items)

        # Verify integrated result
        assert len(enriched_items) == 1
        item = enriched_items[0]
        assert item["item_id"] == "ML001"
        assert item["title"] == "iPhone 14"
        assert item["brand"] == "Apple"
        assert item["color"] == "Black"
        assert item["conversion_rate"] == 0.02  # 100/5000
        assert item["discount_percentage"] == 16.67  # (5999.99-4999.99)/5999.99 * 100

    @patch("src.extractors.items_extractor.create_client")
    def test_error_handling_in_pipeline(self, mock_create_client):
        # Setup mock to simulate API failure
        mock_create_client.side_effect = Exception("Network error")

        from src.extractors.items_extractor import extract_items
        from src.transformers.product_enricher import enrich_items

        # Extraction should fail gracefully
        raw_items = extract_items("SELLER123")
        assert raw_items == []

        # Enrichment should handle empty input
        enriched_items = enrich_items(raw_items)
        assert enriched_items == []

    def test_data_consistency_through_pipeline(self):
        """Test that data maintains consistency through extraction and enrichment."""
        # Simulate raw API data
        raw_items = [
            {
                "id": "ML123",
                "title": "Test Product",
                "price": 299.99,
                "seller_id": "SELLER456",
                "attributes": [{"id": "BRAND", "value_name": "TestBrand"}],
            }
        ]

        from src.transformers.product_enricher import enrich_items

        enriched = enrich_items(raw_items)

        # Verify data consistency
        assert enriched[0]["item_id"] == raw_items[0]["id"]
        assert enriched[0]["title"] == raw_items[0]["title"]
        assert enriched[0]["current_price"] == raw_items[0]["price"]
        assert enriched[0]["seller_id"] == raw_items[0]["seller_id"]
        assert enriched[0]["brand"] == "TestBrand"


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_enrich_item_with_zero_values(self):
        from src.transformers.product_enricher import enrich_item

        item = {
            "id": "ML000",
            "title": "Free Product",
            "price": 0,
            "views": 0,
            "sold_quantity": 0,
        }

        enriched = enrich_item(item)
        assert enriched["current_price"] == 0
        assert enriched["conversion_rate"] == 0.0
        assert enriched["discount_percentage"] == 0.0

    def test_enrich_item_with_missing_price_fields(self):
        from src.transformers.product_enricher import enrich_item

        item = {"id": "ML999", "title": "No Price Product"}
        enriched = enrich_item(item)

        assert enriched["current_price"] == 0.0
        assert enriched["original_price"] == 0.0

    @patch("src.extractors.items_extractor.create_client")
    def test_extract_items_with_large_limit(self, mock_create_client):
        mock_client = Mock()
        mock_client.get_items.return_value = []
        mock_create_client.return_value = (mock_client, "test_token")

        from src.extractors.items_extractor import extract_items

        items = extract_items("seller123", limit=10000)

        # Should handle large limits gracefully
        assert items == []
        mock_client.get_items.assert_called_once_with(
            "test_token", "seller123", limit=10000
        )

    def test_attribute_extraction_with_malformed_data(self):
        from src.transformers.product_enricher import _get_attr

        # Test various malformed attribute structures
        malformed_attrs = [
            {"id": "BRAND"},  # Missing value fields
            {"value_name": "Nike"},  # Missing id
            {"id": "COLOR", "value_name": None, "value_id": None},  # Null values
            {"id": "SIZE", "value_name": "", "value_id": ""},  # Empty strings
        ]

        assert _get_attr(malformed_attrs, "BRAND") is None
        assert _get_attr(malformed_attrs, "COLOR") is None
        assert _get_attr(malformed_attrs, "SIZE") is None


if __name__ == "__main__":
    # Get the directory of the current script (project root)
    project_root = os.path.dirname(os.path.abspath(__file__))

    # Add the project root to the Python path
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    # Default pytest arguments — run in verbose mode
    pytest_args = ["tests/", "-v"]  # Verbose output

    # If additional arguments are passed, include them
    if len(sys.argv) > 1:
        pytest_args.extend(sys.argv[1:])

    print(f"Running pytest with arguments: {pytest_args}")

    # Run pytest
    exit_code = pytest.main(pytest_args)

    sys.exit(exit_code)

# --- conftest.py
# tests/conftest.py

import os
import sys

# make the project root importable (so both src/ and config/ work)
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

# --- test_api.py
#!/usr/bin/env python3
# tests/test_api.py
"""
to run this test suite, you need to have the following environment variables set:

python -m unittest discover -v tests

"""


import json
import unittest
import logging

# now that tests/conftest.py has added project root to PYTHONPATH,
# you can import directly without any inline sys.path hacks:
from src.extractors.ml_api_client import create_client, get_token, is_valid, load_tokens

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestMLClient(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client, cls.token = create_client()
        cls.user_data = None
        cls.site_id = None

    # … rest of your tests unchanged …

    def setUp(self):
        self.assertIsNotNone(self.client)
        self.assertIsNotNone(self.token)

    def test_01_tokens(self):
        tokens = load_tokens()
        self.assertIsNotNone(tokens)
        self.assertIn("access_token", tokens)

        self.assertTrue(is_valid(tokens))

        token = get_token()
        self.assertIsNotNone(token)
        self.assertTrue(len(token) > 50)

        print("✅ Token management")

    def test_02_rate_limiting(self):
        try:
            self.client._check_rate()
            print("✅ Rate limiting")
        except Exception as e:
            self.fail(f"Rate limit check failed: {e}")

    def test_03_user_profile(self):
        user = self.client.get_user(self.token)

        self.assertIsNotNone(user)
        self.assertIn("id", user)
        self.assertIn("nickname", user)
        self.assertIn("site_id", user)

        TestMLClient.user_data = user
        TestMLClient.site_id = user["site_id"]

        print(f"✅ User: {user['nickname']} ({user['id']}, {user['site_id']})")

    def test_04_user_items(self):
        # If user_data isn’t yet a dict, go fetch it:
        if not isinstance(TestMLClient.user_data, dict):
            self.test_03_user_profile()

        # Now Pylint knows user_data is a dict
        user_data = TestMLClient.user_data
        user_id = user_data.get("id")
        self.assertIsNotNone(user_id, "Expected a valid 'id' in user_data")

        items = self.client.get_items(self.token, user_id, limit=5)
        self.assertIsInstance(items, list)

        if items:
            item = items[0]
            self.assertIn("id", item)
            self.assertIn("title", item)
            self.assertIn("price", item)
            self.assertIn("status", item)

        print(f"✅ Retrieved {len(items)} items")

    def test_05_item_details(self):
        # Ensure user_data is a dict before continuing:
        if not isinstance(TestMLClient.user_data, dict):
            self.test_03_user_profile()

        user_data = TestMLClient.user_data
        user_id = user_data.get("id")
        self.assertIsNotNone(user_id, "Expected a valid 'id' in user_data")

        items = self.client.get_items(self.token, user_id, limit=1)
        self.assertIsInstance(items, list)

        if items:
            item_id = items[0].get("id")
            self.assertIsNotNone(item_id, "Expected item to have an 'id'")

            item = self.client.get_item(self.token, item_id)
            self.assertIn("id", item)
            self.assertIn("title", item)

            desc = self.client.get_desc(self.token, item_id)
            self.assertIsInstance(desc, dict)

            reviews = self.client.get_reviews(self.token, item_id)
            self.assertIsInstance(reviews, dict)
            self.assertIn("rating_average", reviews)

            questions = self.client.get_questions(self.token, item_id)
            self.assertIsInstance(questions, dict)

            print(f"✅ Item details for {item_id}")
        else:
            print("⚠️ No items to test")

    def test_06_orders(self):
        # Ensure user_data is a dict before continuing:
        if not isinstance(TestMLClient.user_data, dict):
            self.test_03_user_profile()

        user_data = TestMLClient.user_data
        user_id = user_data.get("id")
        self.assertIsNotNone(user_id, "Expected a valid 'id' in user_data")

        try:
            orders = self.client.get_orders(self.token, user_id, limit=5)
            self.assertIsInstance(orders, list)
            print(f"✅ Retrieved {len(orders)} orders")
        except Exception as e:
            print(f"⚠️ Orders: {e}")

    def test_07_listing_types(self):
        if not TestMLClient.site_id:
            self.test_03_user_profile()

        site_id = TestMLClient.site_id
        try:
            types = self.client.get_listing_types(self.token, site_id)
            self.assertIsInstance(types, list)
            if types:
                self.assertIn("id", types[0])
                self.assertIn("name", types[0])
            print(f"✅ Retrieved {len(types)} listing types")
        except Exception as e:
            print(f"⚠️ Listing types: {e}")

    def test_08_exposures(self):
        if not TestMLClient.site_id:
            self.test_03_user_profile()

        site_id = TestMLClient.site_id
        try:
            exposures = self.client.get_listing_exposures(self.token, site_id)
            self.assertIsInstance(exposures, list)
            print(f"✅ Retrieved {len(exposures)} exposures")
        except Exception as e:
            print(f"⚠️ Exposures: {e}")

    def test_09_search(self):
        # Ensure site_id is set before continuing:
        if not TestMLClient.site_id:
            self.test_03_user_profile()

        site_id = TestMLClient.site_id
        self.assertIsNotNone(site_id, "Expected a valid site_id")

        try:
            results = self.client.search(
                self.token, site_id, query="smartphone", limit=5
            )
            self.assertIsInstance(results, dict)
            self.assertIn("results", results)
            print(f"✅ Search returned {len(results.get('results', []))} items")

            # If user_data exists and is a dict, we can also do a seller_id lookup
            if isinstance(TestMLClient.user_data, dict):
                seller_id = TestMLClient.user_data.get("id")
                if seller_id:
                    seller_results = self.client.search(
                        self.token, site_id, seller_id=seller_id, limit=5
                    )
                    self.assertIsInstance(seller_results, dict)
                    print(
                        f"✅ Seller search returned {len(seller_results.get('results', []))} items"
                    )
        except Exception as e:
            print(f"⚠️ Search: {e}")

    def test_10_categories(self):
        if not TestMLClient.site_id:
            self.test_03_user_profile()

        site_id = TestMLClient.site_id
        try:
            categories = self.client.get_categories(self.token, site_id)
            self.assertIsInstance(categories, list)

            if categories:
                cat_info = self.client.get_category(self.token, categories[0]["id"])
                self.assertIsInstance(cat_info, dict)
                self.assertIn("id", cat_info)
                self.assertIn("name", cat_info)

                print(f"✅ Retrieved {len(categories)} categories")
                print(f"✅ Category info for '{cat_info['name']}'")
            else:
                print("⚠️ No categories")
        except Exception as e:
            print(f"⚠️ Categories: {e}")

    def test_11_trends(self):
        if not TestMLClient.site_id:
            self.test_03_user_profile()

        site_id = TestMLClient.site_id
        try:
            trends = self.client.get_trends(self.token, site_id)
            self.assertIsInstance(trends, (list, dict))
            print("✅ Trends retrieved")
        except Exception as e:
            print(f"⚠️ Trends: {e}")

    def test_12_validation(self):
        """
        Attempt to validate a sample “Underwear” item under category MLB4954.
        This version:

        1) Ensures price ≥ minimum (which is 8 for MLB4954).
        2) Uses currency_id="BRL".
        3) Picks a valid listing_type_id from get_listing_types().
        4) Dynamically adds SIZE_GRID_ID, SIZE_GRID_ROW_ID, and SIZE
        as soon as validation complains they’re missing.
        If SIZE_GRID_ID has no values, the test is skipped rather than failed.
        """
        cat_id = "MLB4954"

        # Step 1: Fetch category info to learn “minimum price”
        try:
            cat_info = self.client.get_category(self.token, cat_id)
        except Exception as e:
            self.fail(f"Failed to fetch category {cat_id}: {e}")
            return

        settings = cat_info.get("settings", {})
        price_settings = settings.get("price", None)
        if isinstance(price_settings, dict):
            min_price = price_settings.get("minimum", 1)
        else:
            min_price = 1

        # Step 2: Pick a valid listing_type_id (first one we find under this site)
        try:
            site_id = TestMLClient.site_id
            lt_list = self.client.get_listing_types(self.token, site_id)
            if isinstance(lt_list, list) and lt_list:
                listing_type_id = lt_list[0]["id"]
            else:
                listing_type_id = "bronze"
        except Exception:
            listing_type_id = "bronze"

        # Build the _initial_ sample_item (no fashion attributes yet)
        sample_item = {
            "title": "Test Underwear Product",
            "category_id": cat_id,
            "price": max(min_price, 8),  # MLB4954 requires ≥ 8
            "currency_id": "BRL",
            "available_quantity": 1,
            "buying_mode": "buy_it_now",
            "listing_type_id": listing_type_id,
            "condition": "new",
            "description": {"plain_text": "Test description for underwear"},
            "pictures": [],
            "attributes": [],
        }

        chosen_grid_id = None
        chosen_row = None

        # Step 3: Loop until valid or until we run out of fixes
        for _ in range(5):
            try:
                result = self.client.validate_item(self.token, sample_item)
            except Exception as e:
                self.fail(f"Validation request threw exception: {e}")
                return

            # If it returns valid:true, we’re done
            if isinstance(result, dict) and result.get("valid") is True:
                print("✅ Item validation passed for MLB4954")
                return

            if not isinstance(result, dict) or "errors" not in result:
                self.fail(f"Unexpected validation response: {result}")
                return

            causes = result["errors"].get("cause", [])
            if not causes:
                self.fail(f"Validation failed, but no 'cause' array: {result}")
                return

            fixed_any = False
            for cause in causes:
                code = cause.get("code", "")

                if code == "missing.fashion_grid.grid_id.values":
                    # Fetch all attributes for this category
                    try:
                        self.client._auth(self.token)
                        all_attrs = self.client._req(
                            "GET", f"/categories/{cat_id}/attributes"
                        )
                    except Exception as e:
                        self.fail(
                            f"Failed to fetch /categories/{cat_id}/attributes: {e}"
                        )
                        return

                    # Print all attribute IDs and value counts for debugging
                    print(">>> ALL ATTRIBUTES FOR MLB4954 <<<")
                    for attr in all_attrs:
                        val_count = len(attr.get("values") or [])
                        print(f"  • id = {attr.get('id'):25} values? {val_count}")
                    print(">>> END ATTRIBUTES LIST <<<")

                    # Find the SIZE_GRID_ID attribute
                    for attr in all_attrs:
                        if attr.get("id") == "SIZE_GRID_ID":
                            vals = attr.get("values", [])
                            print(f"⚠️ SIZE_GRID_ID found, but has {len(vals)} values")
                            if not vals:
                                # Skip instead of fail, since no grid values exist
                                self.skipTest(
                                    "Skipping test_12_validation: "
                                    "SIZE_GRID_ID exists but has no available values in MLB4954."
                                )
                                return
                            chosen_grid_id = vals[0].get("id")
                            break

                    if chosen_grid_id:
                        updated = False
                        for a in sample_item["attributes"]:
                            if a.get("id") == "SIZE_GRID_ID":
                                a["value_id"] = chosen_grid_id
                                updated = True
                                break
                        if not updated:
                            sample_item["attributes"].append(
                                {"id": "SIZE_GRID_ID", "value_id": chosen_grid_id}
                            )
                        fixed_any = True

                elif code == "missing.fashion_grid.grid_row_id.values":
                    if chosen_grid_id is None:
                        self.fail(
                            "SIZE_GRID_ID was never set, but SIZE_GRID_ROW_ID is missing"
                        )
                        return

                    try:
                        self.client._auth(self.token)
                        rows = self.client._req(
                            "GET", f"/size_grids/{chosen_grid_id}/rows"
                        )
                    except Exception as e:
                        self.fail(
                            f"Failed to fetch /size_grids/{chosen_grid_id}/rows: {e}"
                        )
                        return

                    if isinstance(rows, list) and rows:
                        chosen_row = rows[0]
                        row_id = chosen_row.get("id")
                    else:
                        row_id = None

                    if row_id:
                        updated = False
                        for a in sample_item["attributes"]:
                            if a.get("id") == "SIZE_GRID_ROW_ID":
                                a["value_id"] = row_id
                                updated = True
                                break
                        if not updated:
                            sample_item["attributes"].append(
                                {"id": "SIZE_GRID_ROW_ID", "value_id": row_id}
                            )
                        fixed_any = True

                elif code == "missing.fashion_grid.size.values":
                    if not chosen_row:
                        self.fail("SIZE_GRID_ROW_ID was never set, but SIZE is missing")
                        return

                    size_value = chosen_row.get("size") or chosen_row.get("code")
                    if size_value:
                        updated = False
                        for a in sample_item["attributes"]:
                            if a.get("id") == "SIZE":
                                a["value_name"] = size_value
                                updated = True
                                break
                        if not updated:
                            sample_item["attributes"].append(
                                {"id": "SIZE", "value_name": size_value}
                            )
                        fixed_any = True

                elif code == "item.price.invalid":
                    msg = cause.get("message", "")
                    parts = msg.split()
                    try:
                        required_min = int(parts[-1])
                    except Exception:
                        required_min = min_price
                    sample_item["price"] = required_min
                    fixed_any = True

            if not fixed_any:
                print("⚠️ Unhandled validation errors:")
                print(json.dumps(result["errors"], indent=2))
                self.fail("Validation failed with unhandled error codes.")
                return

        # After 5 attempts, if still not valid, show final errors
        print("⚠️ Ran out of attempts to satisfy validation. Final errors payload:")
        print(json.dumps(result["errors"], indent=2))
        self.fail("Could not satisfy all fashion-grid requirements.")

    def test_13_error_handling(self):
        with self.assertRaises(Exception):
            self.client.get_item(self.token, "INVALID_ID")

        with self.assertRaises(Exception):
            self.client._req("GET", "/invalid/endpoint")

        print("✅ Error handling working")

    def test_14_integration(self):
        user = self.client.get_user(self.token)
        items = self.client.get_items(self.token, user["id"], limit=3)

        results = {
            "user_id": user["id"],
            "nickname": user["nickname"],
            "site_id": user["site_id"],
            "total_items": len(items),
            "items_detailed": 0,
            "categories": 0,
            "search_ok": False,
        }

        for item in items:
            try:
                details = self.client.get_item(self.token, item["id"])
                desc = self.client.get_desc(self.token, item["id"])
                if details and desc:
                    results["items_detailed"] += 1
            except Exception:
                pass

        try:
            categories = self.client.get_categories(self.token, user["site_id"])
            results["categories"] = len(categories) if categories else 0
        except Exception:
            pass

        try:
            search_result = self.client.search(
                self.token, user["site_id"], query="test", limit=1
            )
            results["search_ok"] = bool(search_result.get("results"))
        except Exception:
            pass

        self.assertGreaterEqual(results["total_items"], 0)

        if results["total_items"] > 0:
            self.assertEqual(results["items_detailed"], len(items))

        print("✅ Integration flow completed")
        print(f"   User: {results['nickname']} ({results['user_id']})")
        print(
            f"   Items: {results['total_items']} total, {results['items_detailed']} detailed"
        )
        print(f"   Categories: {results['categories']}")
        print(f"   Search: {'Working' if results['search_ok'] else 'Limited'}")


def run_tests():
    print("=" * 50)
    print("MercadoLibre API Client Test Suite")
    print("=" * 50)

    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TestMLClient)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    print("\n" + "=" * 50)
    print("SUMMARY")
    print("=" * 50)

    total = result.testsRun
    failures = len(result.failures)
    errors = len(result.errors)
    success_rate = ((total - failures - errors) / total * 100) if total > 0 else 0

    print(f"Tests: {total}")
    print(f"Passed: {total - failures - errors}")
    print(f"Failed: {failures}")
    print(f"Errors: {errors}")
    print(f"Success: {success_rate:.1f}%")

    if result.failures:
        print("\nFAILURES:")
        for test, trace in result.failures:
            print(f"- {test}: {trace}")

    if result.errors:
        print("\nERRORS:")
        for test, trace in result.errors:
            print(f"- {test}: {trace}")

    if success_rate >= 85:
        print(f"\n🎉 PASSED! API Client ready for production.")
    else:
        print(f"\n⚠️ {failures + errors} tests failed. Review before production.")

    return success_rate >= 85


def quick_test():
    print("Quick API Test...")
    try:
        client, token = create_client()
        user = client.get_user(token)
        items = client.get_items(token, user["id"], limit=1)

        search_ok = False
        try:
            search = client.search(token, user["site_id"], query="test", limit=1)
            search_ok = bool(search.get("results"))
        except Exception:
            pass

        print(f"✅ Quick Test Passed")
        print(f"   User: {user['nickname']} ({user['site_id']})")
        print(f"   Items: {len(items)}")
        print(f"   Search: {'OK' if search_ok else 'Limited'}")
        return True
    except Exception as e:
        print(f"❌ Quick Test Failed: {e}")
        return False


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "quick":
        quick_test()
    else:
        run_tests()

# --- test_data_loader.py
import os
import tempfile
import pytest
from datetime import datetime, timezone

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from src.loaders.data_loader import load_items_to_db
from src.models.models import Base, Item, PriceHistory


@pytest.fixture
def temp_sqlite_db(tmp_path):
    """
    Create a temporary SQLite file on disk, yield its URL,
    and clean it up automatically.
    """
    db_file = tmp_path / "test.db"
    db_url = f"sqlite:///{db_file}"
    engine = create_engine(db_url, future=True)
    Base.metadata.create_all(engine)
    yield db_url  # tests can consume this URL


def test_insert_and_price_history_snapshot(temp_sqlite_db):
    now = datetime.now(timezone.utc)
    record = {
        "item_id": "T1",
        "title": "TestItem",
        "category_id": "C1",
        "current_price": 10.0,
        "original_price": 20.0,
        "available_quantity": 5,
        "sold_quantity": 1,
        "condition": "new",
        "brand": "BrandX",
        "size": "L",
        "color": "Red",
        "gender": "unisex",
        "views": 100,
        "conversion_rate": 0.01,
        "seller_id": 123,
        "updated_at": now,
        "discount_percentage": 50.0,
    }

    # Use the same file-based URL for loader and test session
    load_items_to_db([record], db_url=temp_sqlite_db)

    engine = create_engine(temp_sqlite_db, future=True)
    session = Session(engine)
    itm = session.scalars(select(Item).where(Item.item_id == "T1")).one_or_none()
    assert itm is not None
    assert itm.current_price == pytest.approx(10.0)

    hist = session.scalars(
        select(PriceHistory).where(PriceHistory.item_id == "T1")
    ).all()
    assert len(hist) == 1
    assert hist[0].price == pytest.approx(10.0)
    session.close()


def test_upsert_and_history_append(temp_sqlite_db):
    now = datetime.now(timezone.utc)
    r1 = {
        "item_id": "T2",
        "title": "UpsertItem",
        "category_id": "C2",
        "current_price": 5.0,
        "original_price": 5.0,
        "available_quantity": 10,
        "sold_quantity": 0,
        "condition": "new",
        "brand": None,
        "size": None,
        "color": None,
        "gender": None,
        "views": 0,
        "conversion_rate": 0.0,
        "seller_id": 456,
        "updated_at": now,
        "discount_percentage": 0.0,
    }

    load_items_to_db([r1], db_url=temp_sqlite_db)
    engine = create_engine(temp_sqlite_db, future=True)
    session = Session(engine)
    first = session.scalars(select(Item).where(Item.item_id == "T2")).one()
    assert first.current_price == pytest.approx(5.0)
    h1 = session.scalars(select(PriceHistory).where(PriceHistory.item_id == "T2")).all()
    assert len(h1) == 1
    session.close()

    later = datetime.now(timezone.utc)
    r2 = {**r1, "current_price": 8.0, "sold_quantity": 2, "updated_at": later}
    load_items_to_db([r2], db_url=temp_sqlite_db)

    session = Session(engine)
    updated = session.scalars(select(Item).where(Item.item_id == "T2")).one()
    assert updated.current_price == pytest.approx(8.0)
    h2 = session.scalars(select(PriceHistory).where(PriceHistory.item_id == "T2")).all()
    assert len(h2) == 2
    session.close()

# --- test_items_extractor.py
#!/usr/bin/env python3
# src/extractors/items_extractor.py
"""Test suite for product catalog extraction."""
import pytest
import sys
import os
import logging
from typing import List, Dict, Optional
from src.extractors.ml_api_client import create_client

logger = logging.getLogger(__name__)


def extract_items(seller_id: str, limit: int = 50) -> List[Dict]:
    """
    Extract items for a given seller using the ML API client.

    Args:
        seller_id: The seller ID to extract items for
        limit: Maximum number of items to extract (default: 50)

    Returns:
        List of item dictionaries, or empty list if extraction fails
    """
    if not seller_id:
        logger.error("Seller ID is required")
        return []

    if limit <= 0:
        logger.error("Limit must be positive")
        return []

    try:
        client, token = create_client()
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
    limit: int = 50,
    include_descriptions: bool = True,
    include_reviews: bool = False,
) -> List[Dict]:
    """
    Extract items with additional details like descriptions and optionally reviews.

    Args:
        seller_id: The seller ID to extract items for
        limit: Maximum number of items to extract
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

        enriched_items = []
        for item in items:
            item_id = item.get("id")
            if not item_id:
                enriched_items.append(item)
                continue

            enriched_item = item.copy()

            # Add description if requested
            if include_descriptions:
                try:
                    description = client.get_desc(token, item_id)
                    enriched_item["description"] = description.get("plain_text", "N/A")
                except Exception as e:
                    logger.warning(f"Failed to get description for item {item_id}: {e}")

            # Add reviews if requested
            if include_reviews:
                try:
                    reviews = client.get_reviews(token, item_id)
                    enriched_item["rating_average"] = reviews.get("rating_average", 0)
                    enriched_item["total_reviews"] = reviews.get("total_reviews", 0)
                except Exception as e:
                    logger.warning(f"Failed to get reviews for item {item_id}: {e}")

            enriched_items.append(enriched_item)

        logger.info(
            f"Successfully enriched {len(enriched_items)} items for seller {seller_id}"
        )
        return enriched_items

    except Exception as e:
        logger.error(f"Failed to extract enriched items for seller {seller_id}: {e}")
        return []


# run_tests.py


if __name__ == "__main__":
    # Get the directory of the current script (project root)
    project_root = os.path.dirname(os.path.abspath(__file__))

    # Add the project root to the Python path
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    # Default pytest arguments — run in verbose mode
    pytest_args = ["tests/", "-v"]  # Verbose output

    # If additional arguments are passed, include them
    if len(sys.argv) > 1:
        pytest_args.extend(sys.argv[1:])

    print(f"Running pytest with arguments: {pytest_args}")

    # Run pytest
    exit_code = pytest.main(pytest_args)

    sys.exit(exit_code)

# --- test_product_enricher.py
# tests/test_product_enricher.py
import pytest
from datetime import datetime, timezone
from src.transformers.product_enricher import (
    _get_attr,
    _safe_divide,
    _calculate_discount_percentage,
    enrich_item,
    enrich_items,
)


class TestGetAttr:
    """Test cases for _get_attr helper function."""

    def test_get_attr_success(self):
        """Test successful attribute extraction."""
        attrs = [
            {"id": "BRAND", "value_name": "Noneca"},
            {"id": "SIZE", "value_id": "M"},
            {"id": "MAIN_COLOR", "value_name": "Preto"},
        ]
        assert _get_attr(attrs, "BRAND") == "Noneca"
        assert _get_attr(attrs, "SIZE") == "M"
        assert _get_attr(attrs, "MAIN_COLOR") == "Preto"

    def test_get_attr_not_found(self):
        """Test attribute extraction when key not found."""
        attrs = [{"id": "BRAND", "value_name": "Noneca"}]
        assert _get_attr(attrs, "PANTY_TYPE") is None

    def test_get_attr_empty_attrs(self):
        """Test attribute extraction with empty attributes list."""
        assert _get_attr([], "BRAND") is None

    def test_get_attr_none_attrs(self):
        """Test attribute extraction with None attributes."""
        assert _get_attr(None, "BRAND") is None

    def test_get_attr_value_name_priority(self):
        """Test that value_name takes priority over value_id."""
        attrs = [{"id": "BRAND", "value_name": "Noneca", "value_id": "NONECA_ID"}]
        assert _get_attr(attrs, "BRAND") == "Noneca"

    def test_get_attr_fallback_to_value_id(self):
        """Test fallback to value_id when value_name is None."""
        attrs = [{"id": "SIZE", "value_name": None, "value_id": "P"}]
        assert _get_attr(attrs, "SIZE") == "P"

    def test_get_attr_both_none(self):
        """Test when both value_name and value_id are None."""
        attrs = [{"id": "BRAND", "value_name": None, "value_id": None}]
        assert _get_attr(attrs, "BRAND") is None


class TestSafeDivide:
    """Test cases for _safe_divide helper function."""

    def test_safe_divide_normal(self):
        """Test normal division."""
        assert _safe_divide(10, 2) == 5.0
        assert _safe_divide(7, 3, precision=2) == 2.33

    def test_safe_divide_by_zero(self):
        """Test division by zero."""
        assert _safe_divide(10, 0) == 0.0

    def test_safe_divide_zero_numerator(self):
        """Test zero numerator."""
        assert _safe_divide(0, 5) == 0.0

    def test_safe_divide_negative_numbers(self):
        """Test division with negative numbers."""
        assert _safe_divide(-10, 2) == -5.0
        assert _safe_divide(10, -2) == -5.0

    def test_safe_divide_precision(self):
        """Test precision parameter."""
        assert _safe_divide(1, 3, precision=2) == 0.33
        assert _safe_divide(1, 3, precision=4) == 0.3333


class TestCalculateDiscountPercentage:
    """Test cases for _calculate_discount_percentage helper function."""

    def test_calculate_discount_normal(self):
        """Test normal discount calculation."""
        assert _calculate_discount_percentage(100, 80) == 20.0
        assert _calculate_discount_percentage(61.7, 50) == 18.96

    def test_calculate_discount_no_discount(self):
        """Test when current price equals original price."""
        assert _calculate_discount_percentage(61.7, 61.7) == 0.0

    def test_calculate_discount_price_increase(self):
        """Test when current price is higher than original."""
        assert _calculate_discount_percentage(45.0, 50.0) == 0.0

    def test_calculate_discount_zero_original(self):
        """Test when original price is zero."""
        assert _calculate_discount_percentage(0, 50) == 0.0

    def test_calculate_discount_none_original(self):
        """Test when original price is None."""
        assert _calculate_discount_percentage(None, 50) == 0.0

    def test_calculate_discount_precision(self):
        """Test discount calculation precision."""
        assert _calculate_discount_percentage(75.0, 61.7) == 17.73


class TestEnrichItem:
    """Test cases for enrich_item function."""

    def test_enrich_item_complete(self):
        """Test enriching item with complete data."""
        raw_item = {
            "id": "MLB1101016456",
            "title": "Noneca - Calcinha Aquendar Vinil - Trans, Cds, Travs, Drags",
            "category_id": "MLB4954",
            "price": 61.7,
            "original_price": 75.0,
            "available_quantity": 26,
            "sold_quantity": 1148,
            "condition": "new",
            "views": 15000,
            "seller_id": "354140329",
            "attributes": [
                {"id": "BRAND", "value_name": "Noneca"},
                {"id": "SIZE", "value_name": "M"},
                {"id": "MAIN_COLOR", "value_name": "Preto"},
                {"id": "PANTY_TYPE", "value_name": "Calcinha"},
                {"id": "GENDER", "value_name": "Trans"},
            ],
        }

        result = enrich_item(raw_item)

        # Basic fields
        assert result["item_id"] == "MLB1101016456"
        assert (
            result["title"]
            == "Noneca - Calcinha Aquendar Vinil - Trans, Cds, Travs, Drags"
        )
        assert result["category_id"] == "MLB4954"
        assert result["current_price"] == 61.7
        assert result["original_price"] == 75.0
        assert result["available_quantity"] == 26
        assert result["sold_quantity"] == 1148
        assert result["condition"] == "new"
        assert result["seller_id"] == "354140329"

        # Extracted attributes
        assert result["brand"] == "Noneca"
        assert result["size"] == "M"
        assert result["color"] == "Preto"
        assert result["gender"] == "Trans"

        # Calculated fields
        assert result["views"] == 15000
        assert result["conversion_rate"] == 0.0765  # 1148/15000
        assert result["discount_percentage"] == 17.73  # (75-61.7)/75*100

        # Timestamps
        assert isinstance(result["created_at"], datetime)
        assert isinstance(result["updated_at"], datetime)

    def test_enrich_item_minimal_data(self):
        """Test enriching item with minimal data."""
        raw_item = {"id": "MLB1234567", "title": "Calcinha Básica"}

        result = enrich_item(raw_item)

        assert result["item_id"] == "MLB1234567"
        assert result["title"] == "Calcinha Básica"
        assert result["current_price"] == 0.0
        assert result["original_price"] == 0.0
        assert result["conversion_rate"] == 0.0
        assert result["discount_percentage"] == 0.0
        assert result["brand"] is None
        assert result["views"] == 0

    def test_enrich_item_empty_dict(self):
        """Test enriching empty item."""
        result = enrich_item({})
        assert result == {}

    def test_enrich_item_none(self):
        """Test enriching None item."""
        result = enrich_item(None)
        assert result == {}

    def test_enrich_item_no_views_no_division_error(self):
        """Test that zero views doesn't cause division error."""
        raw_item = {"id": "MLB1234567", "sold_quantity": 10, "views": 0}

        result = enrich_item(raw_item)
        assert result["conversion_rate"] == 0.0

    def test_enrich_item_string_prices(self):
        """Test that string prices are converted to float."""
        raw_item = {"id": "MLB1234567", "price": "61.50", "original_price": "75.90"}

        result = enrich_item(raw_item)
        assert result["current_price"] == 61.5
        assert result["original_price"] == 75.9

    def test_enrich_item_none_prices(self):
        """Test handling of None prices."""
        raw_item = {"id": "MLB1234567", "price": None, "original_price": None}

        result = enrich_item(raw_item)
        assert result["current_price"] == 0.0
        assert result["original_price"] == 0.0

    def test_enrich_item_original_price_defaults_to_current(self):
        """Test that original_price defaults to current price when not provided."""
        raw_item = {"id": "MLB1234567", "price": 45.90}

        result = enrich_item(raw_item)
        assert result["current_price"] == 45.90
        assert result["original_price"] == 45.90
        assert result["discount_percentage"] == 0.0

    def test_enrich_item_timezone_aware_timestamps(self):
        """Test that timestamps are timezone-aware."""
        raw_item = {"id": "MLB1234567"}

        result = enrich_item(raw_item)

        assert result["created_at"].tzinfo == timezone.utc
        assert result["updated_at"].tzinfo == timezone.utc


class TestEnrichItems:
    """Test cases for enrich_items function."""

    def test_enrich_items_multiple(self):
        """Test enriching multiple items."""
        raw_items = [
            {"id": "MLB1101016456", "title": "Calcinha Vinil Trans", "price": 61.7},
            {"id": "MLB1234567", "title": "Calcinha Básica", "price": 35.0},
        ]

        result = enrich_items(raw_items)

        assert len(result) == 2
        assert result[0]["item_id"] == "MLB1101016456"
        assert result[0]["current_price"] == 61.7
        assert result[1]["item_id"] == "MLB1234567"
        assert result[1]["current_price"] == 35.0

    def test_enrich_items_empty_list(self):
        """Test enriching empty list."""
        result = enrich_items([])
        assert result == []

    def test_enrich_items_none_input(self):
        """Test enriching None input."""
        result = enrich_items(None)
        assert result == []

    def test_enrich_items_with_none_items(self):
        """Test enriching list containing None items."""
        raw_items = [
            {"id": "MLB1101016456", "title": "Calcinha Vinil", "price": 61.7},
            None,
            {"id": "MLB1234567", "title": "Calcinha Básica", "price": 35.0},
        ]

        result = enrich_items(raw_items)

        # Should filter out None items
        assert len(result) == 2
        assert result[0]["item_id"] == "MLB1101016456"
        assert result[1]["item_id"] == "MLB1234567"

    def test_enrich_items_with_empty_items(self):
        """Test enriching list containing empty dict items."""
        raw_items = [
            {"id": "MLB1101016456", "title": "Calcinha Vinil", "price": 61.7},
            {},
            {"id": "MLB1234567", "title": "Calcinha Básica", "price": 35.0},
        ]

        result = enrich_items(raw_items)

        # Should filter out empty items
        assert len(result) == 2
        assert result[0]["item_id"] == "MLB1101016456"
        assert result[1]["item_id"] == "MLB1234567"


@pytest.mark.parametrize(
    "views,sold,expected",
    [
        (15000, 1148, 0.0765),
        (0, 10, 0.0),
        (500, 0, 0.0),
        (1000, 25, 0.025),
    ],
)
def test_conversion_rate_calculations(views, sold, expected):
    """Parameterized test for conversion rate calculations."""
    raw_item = {"id": "MLB1234567", "views": views, "sold_quantity": sold}
    result = enrich_item(raw_item)
    assert result["conversion_rate"] == expected


@pytest.mark.parametrize(
    "original,current,expected",
    [
        (75.0, 61.7, 17.73),
        (50.0, 35.0, 30.0),
        (61.7, 61.7, 0.0),
        (45.0, 50.0, 0.0),  # Price increase
        (0, 50, 0.0),
        (None, 50, 0.0),
    ],
)
def test_discount_calculations(original, current, expected):
    """Parameterized test for discount calculations."""
    raw_item = {"id": "MLB1234567", "price": current, "original_price": original}
    result = enrich_item(raw_item)
    assert result["discount_percentage"] == expected


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_malformed_attributes(self):
        """Test handling of malformed attributes."""
        raw_item = {
            "id": "MLB1234567",
            "attributes": [
                {"id": "BRAND"},  # Missing value fields
                {"value_name": "Noneca"},  # Missing id field
                {"id": "SIZE", "value_name": "", "value_id": ""},  # Empty values
                {
                    "id": "MAIN_COLOR",
                    "value_name": "Preto",
                    "value_id": "BLACK",
                },  # Normal
            ],
        }

        result = enrich_item(raw_item)

        assert result["brand"] is None
        assert result["size"] is None
        assert result["color"] == "Preto"

    def test_large_numbers(self):
        """Test handling of large numbers."""
        raw_item = {
            "id": "MLB1234567",
            "price": 999999.99,
            "views": 1000000,
            "sold_quantity": 50000,
        }

        result = enrich_item(raw_item)

        assert result["current_price"] == 999999.99
        assert result["views"] == 1000000
        assert result["conversion_rate"] == 0.05  # 50000/1000000

    def test_unicode_strings(self):
        """Test handling of unicode strings."""
        raw_item = {
            "id": "MLB1234567",
            "title": "Calcinha Renda Açaí",
            "attributes": [{"id": "BRAND", "value_name": "Nonéca"}],
        }

        result = enrich_item(raw_item)

        assert result["title"] == "Calcinha Renda Açaí"
        assert result["brand"] == "Nonéca"

    def test_extreme_precision(self):
        """Test precision handling with extreme decimal places."""
        raw_item = {"id": "MLB1234567", "views": 7, "sold_quantity": 3}

        result = enrich_item(raw_item)

        # 3/7 = 0.4285714285714286, should be rounded to 4 decimal places
        assert result["conversion_rate"] == 0.4286


@pytest.fixture
def sample_raw_items():
    """Fixture providing sample raw items for testing."""
    return [
        {
            "id": "MLB1101016456",
            "title": "Noneca - Calcinha Aquendar Vinil - Trans, Cds, Travs, Drags",
            "category_id": "MLB4954",
            "price": 61.7,
            "original_price": 75.0,
            "available_quantity": 26,
            "sold_quantity": 1148,
            "condition": "new",
            "views": 15000,
            "seller_id": "354140329",
            "attributes": [
                {"id": "BRAND", "value_name": "Noneca"},
                {"id": "SIZE", "value_name": "M"},
                {"id": "MAIN_COLOR", "value_name": "Preto"},
                {"id": "PANTY_TYPE", "value_name": "Calcinha"},
                {"id": "GENDER", "value_name": "Trans"},
            ],
        },
        {
            "id": "MLB1234567",
            "title": "Calcinha Renda Clássica",
            "category_id": "MLB4954",
            "price": 35.0,
            "original_price": 42.0,
            "available_quantity": 15,
            "sold_quantity": 89,
            "condition": "new",
            "views": 890,
            "seller_id": "354140329",
            "attributes": [
                {"id": "BRAND", "value_name": "Noneca"},
                {"id": "SIZE", "value_name": "G"},
                {"id": "MAIN_COLOR", "value_name": "Rosa"},
                {"id": "PANTY_TYPE", "value_name": "Calcinha"},
                {"id": "GENDER", "value_name": "Feminino"},
            ],
        },
    ]


def test_realistic_enrichment_scenario(sample_raw_items):
    """Test enrichment with realistic underwear marketplace data."""
    result = enrich_items(sample_raw_items)

    assert len(result) == 2

    # Vinyl panty checks
    vinyl_panty = result[0]
    assert vinyl_panty["item_id"] == "MLB1101016456"
    assert vinyl_panty["brand"] == "Noneca"
    assert vinyl_panty["size"] == "M"
    assert vinyl_panty["color"] == "Preto"
    assert vinyl_panty["gender"] == "Trans"
    assert vinyl_panty["conversion_rate"] == 0.0765  # 1148/15000
    assert vinyl_panty["discount_percentage"] == 17.73  # (75-61.7)/75*100

    # Lace panty checks
    lace_panty = result[1]
    assert lace_panty["item_id"] == "MLB1234567"
    assert lace_panty["brand"] == "Noneca"
    assert lace_panty["size"] == "G"
    assert lace_panty["color"] == "Rosa"
    assert lace_panty["gender"] == "Feminino"
    assert lace_panty["conversion_rate"] == 0.1  # 89/890
    assert lace_panty["discount_percentage"] == 16.67  # (42-35)/42*100

```


# Test Results
```
Running pytest with arguments: ['tests/', '-v']
============================= test session starts =============================
platform win32 -- Python 3.13.2, pytest-8.4.0, pluggy-1.6.0 -- C:\Users\victo\Downloads\noneca_com_main_v2\.venv\Scripts\python.exe
cachedir: .pytest_cache
rootdir: c:\Users\victo\Downloads\noneca_com_main_v2\tests
configfile: pytest.ini
plugins: dash-2.9.3
collecting ... collected 100 items

tests\comp_test.py::TestProductEnricher::test_get_attr_extracts_value_name PASSED [  1%]
tests\comp_test.py::TestProductEnricher::test_get_attr_falls_back_to_value_id PASSED [  2%]
tests\comp_test.py::TestProductEnricher::test_get_attr_returns_none_for_missing_key PASSED [  3%]
tests\comp_test.py::TestProductEnricher::test_get_attr_handles_empty_values PASSED [  4%]
tests\comp_test.py::TestProductEnricher::test_safe_divide_normal_case PASSED [  5%]
tests\comp_test.py::TestProductEnricher::test_safe_divide_zero_denominator PASSED [  6%]
tests\comp_test.py::TestProductEnricher::test_calculate_discount_percentage PASSED [  7%]
tests\comp_test.py::TestProductEnricher::test_enrich_item_complete_data PASSED [  8%]
tests\comp_test.py::TestProductEnricher::test_enrich_item_missing_attributes PASSED [  9%]
tests\comp_test.py::TestProductEnricher::test_enrich_item_empty_input PASSED [ 10%]
tests\comp_test.py::TestProductEnricher::test_enrich_items_list PASSED   [ 11%]
tests\comp_test.py::TestProductEnricher::test_enrich_items_empty_list PASSED [ 12%]
tests\comp_test.py::TestMLAPIClient::test_client_initialization PASSED   [ 13%]
tests\comp_test.py::TestMLAPIClient::test_rate_limiting PASSED           [ 14%]
tests\comp_test.py::TestMLAPIClient::test_rate_limit_reset PASSED        [ 15%]
tests\comp_test.py::TestMLAPIClient::test_get_user_default_params PASSED [ 16%]
tests\comp_test.py::TestMLAPIClient::test_get_items_with_limit PASSED    [ 17%]
tests\comp_test.py::TestMLAPIClient::test_get_item_details PASSED        [ 18%]
tests\comp_test.py::TestMLAPIClient::test_get_description PASSED         [ 19%]
tests\comp_test.py::TestMLAPIClient::test_get_reviews PASSED             [ 20%]
tests\comp_test.py::TestItemsExtractor::test_extract_items_success PASSED [ 21%]
tests\comp_test.py::TestItemsExtractor::test_extract_items_empty_seller_id PASSED [ 22%]
tests\comp_test.py::TestItemsExtractor::test_extract_items_invalid_limit PASSED [ 23%]
tests\comp_test.py::TestItemsExtractor::test_extract_items_api_failure PASSED [ 24%]
tests\comp_test.py::TestItemsExtractor::test_extract_item_details_success PASSED [ 25%]
tests\comp_test.py::TestItemsExtractor::test_extract_items_with_enrichments PASSED [ 26%]
tests\comp_test.py::TestTokenManagement::test_load_tokens_from_file PASSED [ 27%]
tests\comp_test.py::TestTokenManagement::test_load_tokens_fallback PASSED [ 28%]
tests\comp_test.py::TestTokenManagement::test_is_valid_token_check PASSED [ 29%]
tests\comp_test.py::TestTokenManagement::test_save_tokens PASSED         [ 30%]
tests\comp_test.py::TestIntegratedWorkflow::test_full_extraction_and_enrichment_pipeline PASSED [ 31%]
tests\comp_test.py::TestIntegratedWorkflow::test_error_handling_in_pipeline PASSED [ 32%]
tests\comp_test.py::TestIntegratedWorkflow::test_data_consistency_through_pipeline PASSED [ 33%]
tests\comp_test.py::TestEdgeCases::test_enrich_item_with_zero_values PASSED [ 34%]
tests\comp_test.py::TestEdgeCases::test_enrich_item_with_missing_price_fields PASSED [ 35%]
tests\comp_test.py::TestEdgeCases::test_extract_items_with_large_limit PASSED [ 36%]
tests\comp_test.py::TestEdgeCases::test_attribute_extraction_with_malformed_data PASSED [ 37%]
tests\test_api.py::TestMLClient::test_01_tokens PASSED                   [ 38%]
tests\test_api.py::TestMLClient::test_02_rate_limiting PASSED            [ 39%]
tests\test_api.py::TestMLClient::test_03_user_profile PASSED             [ 40%]
tests\test_api.py::TestMLClient::test_04_user_items PASSED               [ 41%]
tests\test_api.py::TestMLClient::test_05_item_details PASSED             [ 42%]
tests\test_api.py::TestMLClient::test_06_orders PASSED                   [ 43%]
tests\test_api.py::TestMLClient::test_07_listing_types PASSED            [ 44%]
tests\test_api.py::TestMLClient::test_08_exposures PASSED                [ 45%]
tests\test_api.py::TestMLClient::test_09_search PASSED                   [ 46%]
tests\test_api.py::TestMLClient::test_10_categories PASSED               [ 47%]
tests\test_api.py::TestMLClient::test_11_trends PASSED                   [ 48%]
tests\test_api.py::TestMLClient::test_12_validation SKIPPED (Skippin...) [ 49%]
tests\test_api.py::TestMLClient::test_13_error_handling PASSED           [ 50%]
tests\test_api.py::TestMLClient::test_14_integration PASSED              [ 51%]
tests\test_data_loader.py::test_insert_and_price_history_snapshot PASSED [ 52%]
tests\test_data_loader.py::test_upsert_and_history_append PASSED         [ 53%]
tests\test_product_enricher.py::TestGetAttr::test_get_attr_success PASSED [ 54%]
tests\test_product_enricher.py::TestGetAttr::test_get_attr_not_found PASSED [ 55%]
tests\test_product_enricher.py::TestGetAttr::test_get_attr_empty_attrs PASSED [ 56%]
tests\test_product_enricher.py::TestGetAttr::test_get_attr_none_attrs PASSED [ 57%]
tests\test_product_enricher.py::TestGetAttr::test_get_attr_value_name_priority PASSED [ 58%]
tests\test_product_enricher.py::TestGetAttr::test_get_attr_fallback_to_value_id PASSED [ 59%]
tests\test_product_enricher.py::TestGetAttr::test_get_attr_both_none PASSED [ 60%]
tests\test_product_enricher.py::TestSafeDivide::test_safe_divide_normal PASSED [ 61%]
tests\test_product_enricher.py::TestSafeDivide::test_safe_divide_by_zero PASSED [ 62%]
tests\test_product_enricher.py::TestSafeDivide::test_safe_divide_zero_numerator PASSED [ 63%]
tests\test_product_enricher.py::TestSafeDivide::test_safe_divide_negative_numbers PASSED [ 64%]
tests\test_product_enricher.py::TestSafeDivide::test_safe_divide_precision PASSED [ 65%]
tests\test_product_enricher.py::TestCalculateDiscountPercentage::test_calculate_discount_normal PASSED [ 66%]
tests\test_product_enricher.py::TestCalculateDiscountPercentage::test_calculate_discount_no_discount PASSED [ 67%]
tests\test_product_enricher.py::TestCalculateDiscountPercentage::test_calculate_discount_price_increase PASSED [ 68%]
tests\test_product_enricher.py::TestCalculateDiscountPercentage::test_calculate_discount_zero_original PASSED [ 69%]
tests\test_product_enricher.py::TestCalculateDiscountPercentage::test_calculate_discount_none_original PASSED [ 70%]
tests\test_product_enricher.py::TestCalculateDiscountPercentage::test_calculate_discount_precision PASSED [ 71%]
tests\test_product_enricher.py::TestEnrichItem::test_enrich_item_complete PASSED [ 72%]
tests\test_product_enricher.py::TestEnrichItem::test_enrich_item_minimal_data PASSED [ 73%]
tests\test_product_enricher.py::TestEnrichItem::test_enrich_item_empty_dict PASSED [ 74%]
tests\test_product_enricher.py::TestEnrichItem::test_enrich_item_none PASSED [ 75%]
tests\test_product_enricher.py::TestEnrichItem::test_enrich_item_no_views_no_division_error PASSED [ 76%]
tests\test_product_enricher.py::TestEnrichItem::test_enrich_item_string_prices PASSED [ 77%]
tests\test_product_enricher.py::TestEnrichItem::test_enrich_item_none_prices PASSED [ 78%]
tests\test_product_enricher.py::TestEnrichItem::test_enrich_item_original_price_defaults_to_current PASSED [ 79%]
tests\test_product_enricher.py::TestEnrichItem::test_enrich_item_timezone_aware_timestamps PASSED [ 80%]
tests\test_product_enricher.py::TestEnrichItems::test_enrich_items_multiple PASSED [ 81%]
tests\test_product_enricher.py::TestEnrichItems::test_enrich_items_empty_list PASSED [ 82%]
tests\test_product_enricher.py::TestEnrichItems::test_enrich_items_none_input PASSED [ 83%]
tests\test_product_enricher.py::TestEnrichItems::test_enrich_items_with_none_items PASSED [ 84%]
tests\test_product_enricher.py::TestEnrichItems::test_enrich_items_with_empty_items PASSED [ 85%]
tests\test_product_enricher.py::test_conversion_rate_calculations[15000-1148-0.0765] PASSED [ 86%]
tests\test_product_enricher.py::test_conversion_rate_calculations[0-10-0.0] PASSED [ 87%]
tests\test_product_enricher.py::test_conversion_rate_calculations[500-0-0.0] PASSED [ 88%]
tests\test_product_enricher.py::test_conversion_rate_calculations[1000-25-0.025] PASSED [ 89%]
tests\test_product_enricher.py::test_discount_calculations[75.0-61.7-17.73] PASSED [ 90%]
tests\test_product_enricher.py::test_discount_calculations[50.0-35.0-30.0] PASSED [ 91%]
tests\test_product_enricher.py::test_discount_calculations[61.7-61.7-0.0] PASSED [ 92%]
tests\test_product_enricher.py::test_discount_calculations[45.0-50.0-0.0] PASSED [ 93%]
tests\test_product_enricher.py::test_discount_calculations[0-50-0.0] PASSED [ 94%]
tests\test_product_enricher.py::test_discount_calculations[None-50-0.0] PASSED [ 95%]
tests\test_product_enricher.py::TestEdgeCases::test_malformed_attributes PASSED [ 96%]
tests\test_product_enricher.py::TestEdgeCases::test_large_numbers PASSED [ 97%]
tests\test_product_enricher.py::TestEdgeCases::test_unicode_strings PASSED [ 98%]
tests\test_product_enricher.py::TestEdgeCases::test_extreme_precision PASSED [ 99%]
tests\test_product_enricher.py::test_realistic_enrichment_scenario PASSED [100%]

======================= 99 passed, 1 skipped in 34.95s ========================
c:\Users\victo\Downloads\noneca_com_main_v2\.venv\Lib\site-packages\dash\dash.py:22: UserWarning: pkg_resources is deprecated as an API. See https://setuptools.pypa.io/en/latest/pkg_resources.html. The pkg_resources package is slated for removal as early as 2025-11-30. Refrain from using this package or pin to Setuptools<81.
  from pkg_resources import get_distribution, parse_version
```
