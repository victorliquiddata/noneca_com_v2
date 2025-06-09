#!/usr/bin/env python3
# smoke_test.py
"""
Smoke test script for integrated items and orders extraction.
Tests pagination limits, database loading, and data integrity.
"""

import os
import sys
import json
import logging
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.extractors.items_extractor import extract_items, extract_items_with_enrichments
from src.extractors.orders_extractor import extract_orders
from src.transformers.product_enricher import enrich_items
from src.transformers.order_enricher import enrich_orders
from src.loaders.data_loader import load_items_to_db, load_orders_to_db
from src.extractors.ml_api_client import create_client

# Configure logging
# Replace the existing logging.basicConfig block with this:
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# File Handler
file_handler = logging.FileHandler("smoke_test.log", encoding="utf-8")
file_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
)
logger.addHandler(file_handler)

# Stream Handler (console output)
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
)
logger.addHandler(stream_handler)

# Ensure console output uses UTF-8 if it's not already set by your environment.
# You might need to set the environment variable PYTHONIOENCODING='utf-8'
# before running your script, especially on Windows, for the console to
# correctly display all characters.


class SmokeTestConfig:
    """Test configuration parameters."""

    # Database settings
    TEST_DB_PATH = "sqlite:///./data/smoke_test_analytics.db"
    BACKUP_DB_PATH = "sqlite:///./data/smoke_test_backup.db"

    # API limits for testing
    MAX_ITEMS_TO_EXTRACT = None  # None = all available
    MAX_ORDERS_TO_EXTRACT = None  # Last 200 orders
    PAGINATION_LIMIT = 50  # API pagination limit

    # Test seller ID - you'll need to replace this
    TEST_SELLER_ID = "354140329"  # Replace with actual seller ID

    # Date range for orders:
    # Option 1: Retrieve for the maximum range the API allows by setting to None.
    #           The 'extract_orders' function can handle None, relying on the API's
    #           default behavior for the full history (which you mentioned is ~1 year).
    DATE_FROM = None  # Set to None to attempt to retrieve all available history.
    DATE_TO = None  # Set to None to retrieve up to the current date.

    # Option 2 (If you want precisely 365 days, ignoring API's potential larger default):
    # DATE_FROM = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%dT%H:%M:%S.000-03:00")
    # DATE_TO = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000-03:00")


"""""
    COMMENTED FOR NOW, UNCOMMENT WHEN NEW TESTS ARE REQUIRED
    # Date range for orders (last 360 days)
    # In SmokeTestConfig
    DATE_FROM = (datetime.now() - timedelta(days=360)).strftime(
        "%Y-%m-%dT%H:%M:%S.000-03:00"
    )
    DATE_TO = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000-03:00")
"""


class SmokeTestRunner:
    """Main smoke test runner."""

    def __init__(self, config: SmokeTestConfig):
        self.config = config
        self.results = {
            "start_time": datetime.now(),
            "items": {"extracted": 0, "enriched": 0, "loaded": 0, "errors": []},
            "orders": {"extracted": 0, "enriched": 0, "loaded": 0, "errors": []},
            "database": {"tables_created": False, "integrity_checks": []},
            "pagination": {"items_pages": 0, "orders_pages": 0},
            "performance": {
                "extraction_time": 0,
                "enrichment_time": 0,
                "loading_time": 0,
            },
        }

    def setup_test_environment(self):
        """Setup test database and environment."""
        logger.info("Setting up test environment...")

        # Create data directory if it doesn't exist
        os.makedirs("./data", exist_ok=True)

        # Remove existing test database
        if os.path.exists(self.config.TEST_DB_PATH):
            os.remove(self.config.TEST_DB_PATH)
            logger.info(f"Removed existing test database: {self.config.TEST_DB_PATH}")

        # Test API connection
        try:
            client, token = create_client()
            user_info = client.get_user(token)
            logger.info(
                f"API connection successful. User: {user_info.get('nickname', 'Unknown')}"
            )
            return True
        except Exception as e:
            logger.error(f"API connection failed: {e}")
            self.results["items"]["errors"].append(f"API connection failed: {e}")
            return False

    def test_items_extraction(self):
        """Test items extraction with pagination."""
        logger.info("=" * 60)
        logger.info("TESTING ITEMS EXTRACTION")
        logger.info("=" * 60)

        start_time = datetime.now()

        try:
            # Test basic extraction
            logger.info(f"Extracting items for seller: {self.config.TEST_SELLER_ID}")
            logger.info(f"Max items limit: {self.config.MAX_ITEMS_TO_EXTRACT or 'ALL'}")

            raw_items = extract_items(
                seller_id=self.config.TEST_SELLER_ID,
                limit=self.config.MAX_ITEMS_TO_EXTRACT,
            )

            self.results["items"]["extracted"] = len(raw_items)
            logger.info(f"✓ Extracted {len(raw_items)} raw items")

            if not raw_items:
                logger.warning("No items extracted - check seller ID or API access")
                return

            # Test enrichment
            logger.info("Enriching items...")
            enriched_items = enrich_items(raw_items)
            self.results["items"]["enriched"] = len(enriched_items)
            logger.info(f"✓ Enriched {len(enriched_items)} items")

            # Test enhanced extraction with enrichments
            logger.info("Testing enhanced extraction with descriptions and reviews...")
            enhanced_items = extract_items_with_enrichments(
                seller_id=self.config.TEST_SELLER_ID,
                limit=min(10, len(raw_items)),  # Test with first 10 items
                include_descriptions=True,
                include_reviews=True,
            )
            logger.info(
                f"✓ Enhanced extraction completed for {len(enhanced_items)} items"
            )

            # Test database loading
            logger.info("Loading items to database...")
            load_items_to_db(enriched_items, self.config.TEST_DB_PATH)
            self.results["items"]["loaded"] = len(enriched_items)
            logger.info(f"✓ Loaded {len(enriched_items)} items to database")

            # Sample item analysis
            if enriched_items:
                sample_item = enriched_items[0]
                logger.info(f"Sample item: {sample_item.get('title', 'N/A')[:50]}...")
                logger.info(f"  - Price: {sample_item.get('current_price', 'N/A')}")
                logger.info(f"  - Brand: {sample_item.get('brand', 'N/A')}")
                logger.info(f"  - Seller: {sample_item.get('seller_id', 'N/A')}")

        except Exception as e:
            error_msg = f"Items extraction failed: {e}"
            logger.error(error_msg)
            self.results["items"]["errors"].append(error_msg)

        extraction_time = (datetime.now() - start_time).total_seconds()
        self.results["performance"]["extraction_time"] += extraction_time
        logger.info(f"Items extraction completed in {extraction_time:.2f} seconds")

    def test_orders_extraction(self):
        """Test orders extraction with pagination."""
        logger.info("=" * 60)
        logger.info("TESTING ORDERS EXTRACTION")
        logger.info("=" * 60)

        start_time = datetime.now()

        try:
            logger.info(f"Extracting orders for seller: {self.config.TEST_SELLER_ID}")
            logger.info(f"Date range: {self.config.DATE_FROM} to {self.config.DATE_TO}")
            logger.info(f"Max orders limit: {self.config.MAX_ORDERS_TO_EXTRACT}")

            raw_orders = extract_orders(
                seller_id=self.config.TEST_SELLER_ID,
                date_from=self.config.DATE_FROM,
                date_to=self.config.DATE_TO,
                limit=self.config.PAGINATION_LIMIT,
                max_records=self.config.MAX_ORDERS_TO_EXTRACT,
            )

            self.results["orders"]["extracted"] = len(raw_orders)
            logger.info(f"✓ Extracted {len(raw_orders)} raw orders")

            if not raw_orders:
                logger.warning(
                    "No orders extracted - check date range or seller activity"
                )
                return

            # Test enrichment
            logger.info("Enriching orders...")
            enriched_orders = enrich_orders(raw_orders)
            self.results["orders"]["enriched"] = len(enriched_orders)
            logger.info(f"✓ Enriched {len(enriched_orders)} orders")

            # Test database loading
            logger.info("Loading orders to database...")
            load_orders_to_db(enriched_orders, self.config.TEST_DB_PATH)
            self.results["orders"]["loaded"] = len(enriched_orders)
            logger.info(f"✓ Loaded {len(enriched_orders)} orders to database")

            # Sample order analysis
            if enriched_orders:
                sample_order = enriched_orders[0]
                logger.info(f"Sample order: {sample_order.get('order_id', 'N/A')}")
                logger.info(f"  - Status: {sample_order.get('status', 'N/A')}")
                logger.info(f"  - Total: {sample_order.get('total_amount', 'N/A')}")
                logger.info(f"  - Items: {sample_order.get('total_items', 'N/A')}")
                logger.info(f"  - Date: {sample_order.get('date_created', 'N/A')}")

        except Exception as e:
            error_msg = f"Orders extraction failed: {e}"
            logger.error(error_msg)
            self.results["orders"]["errors"].append(error_msg)

        extraction_time = (datetime.now() - start_time).total_seconds()
        self.results["performance"]["extraction_time"] += extraction_time
        logger.info(f"Orders extraction completed in {extraction_time:.2f} seconds")

    def test_database_integrity(self):
        """Test database integrity and relationships."""
        logger.info("=" * 60)
        logger.info("TESTING DATABASE INTEGRITY")
        logger.info("=" * 60)

        try:
            db_path = self.config.TEST_DB_PATH.replace("sqlite:///", "")
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            # Check if tables exist
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [row[0] for row in cursor.fetchall()]
            logger.info(f"Created tables: {', '.join(tables)}")

            expected_tables = [
                "items",
                "sellers",
                "orders",
                "buyers",
                "order_items",
                "price_history",
            ]
            missing_tables = set(expected_tables) - set(tables)
            if missing_tables:
                logger.warning(f"Missing tables: {', '.join(missing_tables)}")
            else:
                logger.info("✓ All expected tables created")
                self.results["database"]["tables_created"] = True

            # Check record counts
            table_counts = {}
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                table_counts[table] = count
                logger.info(f"  {table}: {count} records")

            # Integrity checks
            checks = []

            # Check items-sellers relationship
            if "items" in tables and "sellers" in tables:
                cursor.execute(
                    """
                    SELECT COUNT(*) FROM items i 
                    LEFT JOIN sellers s ON i.seller_id = s.seller_id 
                    WHERE i.seller_id IS NOT NULL AND s.seller_id IS NULL
                """
                )
                orphaned_items = cursor.fetchone()[0]
                if orphaned_items == 0:
                    checks.append("✓ Items-sellers relationship intact")
                else:
                    checks.append(
                        f"⚠ {orphaned_items} items with missing seller references"
                    )

            # Check orders-buyers relationship
            if "orders" in tables and "buyers" in tables:
                cursor.execute(
                    """
                    SELECT COUNT(*) FROM orders o 
                    LEFT JOIN buyers b ON o.buyer_id = b.buyer_id 
                    WHERE o.buyer_id IS NOT NULL AND b.buyer_id IS NULL
                """
                )
                orphaned_orders = cursor.fetchone()[0]
                if orphaned_orders == 0:
                    checks.append("✓ Orders-buyers relationship intact")
                else:
                    checks.append(
                        f"⚠ {orphaned_orders} orders with missing buyer references"
                    )

            # Check order_items relationships
            if "order_items" in tables and "orders" in tables and "items" in tables:
                cursor.execute(
                    """
                    SELECT COUNT(*) FROM order_items oi 
                    LEFT JOIN orders o ON oi.order_id = o.order_id 
                    WHERE o.order_id IS NULL
                """
                )
                orphaned_order_items = cursor.fetchone()[0]
                if orphaned_order_items == 0:
                    checks.append("✓ Order items-orders relationship intact")
                else:
                    checks.append(
                        f"⚠ {orphaned_order_items} order items with missing order references"
                    )

            self.results["database"]["integrity_checks"] = checks
            for check in checks:
                logger.info(f"  {check}")

            conn.close()

        except Exception as e:
            error_msg = f"Database integrity check failed: {e}"
            logger.error(error_msg)
            self.results["database"]["integrity_checks"].append(error_msg)

    def test_pagination_behavior(self):
        """Test pagination behavior and limits."""
        logger.info("=" * 60)
        logger.info("TESTING PAGINATION BEHAVIOR")
        logger.info("=" * 60)

        try:
            client, token = create_client()

            # Test items pagination with small limit
            logger.info("Testing items pagination with limit=5...")
            small_batch = client.get_items(token, self.config.TEST_SELLER_ID, limit=5)
            logger.info(f"✓ Small batch extraction: {len(small_batch)} items")

            # Test orders pagination with small limit
            logger.info("Testing orders pagination with limit=10...")
            small_orders = extract_orders(
                seller_id=self.config.TEST_SELLER_ID,
                date_from=self.config.DATE_FROM,
                date_to=self.config.DATE_TO,
                limit=10,
                max_records=10,
            )
            logger.info(f"✓ Small orders batch: {len(small_orders)} orders")

            # Log pagination metrics
            self.results["pagination"]["items_pages"] = (
                len(small_batch) // self.config.PAGINATION_LIMIT + 1
            )
            self.results["pagination"]["orders_pages"] = len(small_orders) // 10 + 1

        except Exception as e:
            error_msg = f"Pagination test failed: {e}"
            logger.error(error_msg)

    def generate_report(self):
        """Generate comprehensive test report."""
        logger.info("=" * 60)
        logger.info("SMOKE TEST REPORT")
        logger.info("=" * 60)

        self.results["end_time"] = datetime.now()
        duration = (
            self.results["end_time"] - self.results["start_time"]
        ).total_seconds()

        # Summary
        logger.info(f"Test Duration: {duration:.2f} seconds")
        logger.info(f"Test Database: {self.config.TEST_DB_PATH}")
        logger.info("")

        # Items results
        items = self.results["items"]
        logger.info("ITEMS EXTRACTION:")
        logger.info(f"  Extracted: {items['extracted']}")
        logger.info(f"  Enriched:  {items['enriched']}")
        logger.info(f"  Loaded:    {items['loaded']}")
        if items["errors"]:
            logger.info(f"  Errors:    {len(items['errors'])}")

        # Orders results
        orders = self.results["orders"]
        logger.info("")
        logger.info("ORDERS EXTRACTION:")
        logger.info(f"  Extracted: {orders['extracted']}")
        logger.info(f"  Enriched:  {orders['enriched']}")
        logger.info(f"  Loaded:    {orders['loaded']}")
        if orders["errors"]:
            logger.info(f"  Errors:    {len(orders['errors'])}")

        # Database results
        logger.info("")
        logger.info("DATABASE:")
        logger.info(f"  Tables Created: {self.results['database']['tables_created']}")
        for check in self.results["database"]["integrity_checks"]:
            logger.info(f"  {check}")

        # Performance
        perf = self.results["performance"]
        logger.info("")
        logger.info("PERFORMANCE:")
        logger.info(f"  Total Extraction Time: {perf['extraction_time']:.2f}s")

        # Overall status
        logger.info("")
        total_errors = len(items["errors"]) + len(orders["errors"])
        if total_errors == 0 and items["loaded"] > 0:
            logger.info("✅ SMOKE TEST PASSED")
        else:
            logger.info("❌ SMOKE TEST FAILED")
            if total_errors > 0:
                logger.info(f"   {total_errors} errors encountered")
            if items["loaded"] == 0 and orders["loaded"] == 0:
                logger.info("   No data loaded to database")

        # Save results to JSON
        results_file = "smoke_test_results.json"
        with open(results_file, "w") as f:
            # Convert datetime objects to strings for JSON serialization
            json_results = self.results.copy()
            json_results["start_time"] = self.results["start_time"].isoformat()
            json_results["end_time"] = self.results["end_time"].isoformat()
            json.dump(json_results, f, indent=2, default=str)

        logger.info(f"Detailed results saved to: {results_file}")

    def run_smoke_test(self):
        """Run complete smoke test suite."""
        logger.info("🚀 Starting Smoke Test Suite")
        logger.info(f"Test configuration:")
        logger.info(f"  Seller ID: {self.config.TEST_SELLER_ID}")
        logger.info(f"  Max Items: {self.config.MAX_ITEMS_TO_EXTRACT or 'ALL'}")
        logger.info(f"  Max Orders: {self.config.MAX_ORDERS_TO_EXTRACT}")
        logger.info(f"  Date Range: {self.config.DATE_FROM} to {self.config.DATE_TO}")
        logger.info("")

        # Setup
        if not self.setup_test_environment():
            logger.error("Environment setup failed. Aborting test.")
            return False

        # Run tests
        self.test_pagination_behavior()
        self.test_items_extraction()
        self.test_orders_extraction()
        self.test_database_integrity()

        # Generate report
        self.generate_report()

        return True


def main():
    """Main entry point."""
    # Validate configuration
    config = SmokeTestConfig()

    if config.TEST_SELLER_ID == "YOUR_SELLER_ID_HERE":
        logger.error("❌ Please update TEST_SELLER_ID in SmokeTestConfig")
        logger.error("   You can find your seller ID by running:")
        logger.error(
            "   python -c \"from src.extractors.ml_api_client import *; c,t=create_client(); print(c.get_user(t)['id'])\""
        )
        sys.exit(1)

    # Run smoke test
    runner = SmokeTestRunner(config)
    success = runner.run_smoke_test()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
