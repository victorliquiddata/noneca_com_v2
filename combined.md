```python
# --- .\main.py
#!/usr/bin/env python3
# main.py
"""

Improved Main ETL pipeline orchestrator for Noneca.com Mercado Livre Analytics Platform.
Extracts product data and order data, enriches both, and loads into database.

🚀 Usage Examples
Basic Usage (Multi-seller mode):

python main.py

Single seller with config file:

python main.py config.json

Single seller, specific pipeline:

python main.py 354140329 items    # Items only

python main.py 354140329 orders   # Orders only

python main.py 354140329 full     # Both pipelines

"""

import sys
import logging
import os
import json
from typing import List, Dict, Optional, Any
from datetime import datetime, timedelta
from pathlib import Path
from dataclasses import dataclass, asdict

from src.extractors.items_extractor import extract_items_with_enrichments
from src.extractors.orders_extractor import extract_orders
from src.transformers.product_enricher import enrich_items
from src.transformers.order_enricher import enrich_orders
from src.loaders.data_loader import load_items_to_db, load_orders_to_db
from src.extractors.ml_api_client import create_client


@dataclass
class PipelineConfig:
    """Configuration class for ETL pipeline."""

    # Database settings
    db_url: str = "sqlite:///./data/noneca_analytics.db"
    backup_db_url: str = "sqlite:///./data/noneca_analytics_backup.db"

    # API limits (respecting Mercado Libre API constraints)
    max_items_per_seller: Optional[int] = None  # None = all available
    max_orders_per_seller: Optional[int] = None  # None = all available
    api_pagination_limit: int = 50  # Max 50 per API call

    # Orders date range (None = API default range, typically ~1 year)
    orders_date_from: Optional[str] = None
    orders_date_to: Optional[str] = None

    # Items enrichment options
    include_descriptions: bool = True
    include_reviews: bool = False

    # Default sellers for multi-seller runs
    default_sellers: List[str] = None

    # Logging configuration
    log_level: str = "INFO"
    log_to_file: bool = True
    log_file: str = "pipeline.log"

    def __post_init__(self):
        """Initialize default values after dataclass creation."""
        if self.default_sellers is None:
            self.default_sellers = [
                "354140329",  # Replace with actual seller IDs
            ]

        # Leave dates as None to use API's default range (typically 1 year)
        # Comment out the date setting logic to use API defaults

        # To go back to stipulating a date range:
        # Simply uncomment those lines (change days=30 to days=365 for 1 year)
        # Don't comment anything else

        # if self.orders_date_from is None:
        #     self.orders_date_from = (datetime.now() - timedelta(days=365)).strftime(
        #         "%Y-%m-%dT00:00:00.000Z"
        #     )
        # if self.orders_date_to is None:
        #     self.orders_date_to = datetime.now().strftime("%Y-%m-%dT23:59:59.999Z")

        # Update log file path to include subfolder
        self.log_file = f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    def save_to_file(self, filepath: str = None):
        """Save configuration to JSON file in organized structure."""
        if filepath is None:
            # Create config directory and use default path
            os.makedirs("config", exist_ok=True)
            filepath = "config/pipeline_configs.json"

        # Ensure directory exists
        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        with open(filepath, "w") as f:
            json.dump(asdict(self), f, indent=2, default=str)

    @classmethod
    def load_from_file(cls, filepath: str) -> "PipelineConfig":
        """Load configuration from JSON file."""
        with open(filepath, "r") as f:
            data = json.load(f)
        return cls(**data)


class ImprovedPipelineLogger:
    """Enhanced logging setup for the pipeline."""

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        """Setup comprehensive logging with file and console handlers."""
        logger = logging.getLogger("pipeline")
        logger.setLevel(getattr(logging, self.config.log_level.upper()))

        # Clear existing handlers
        logger.handlers.clear()

        # Create formatters
        detailed_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        simple_formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s", datefmt="%H:%M:%S"
        )

        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(simple_formatter)
        console_handler.setLevel(logging.INFO)
        logger.addHandler(console_handler)

        # File handler (if enabled) - organized in subfolder
        if self.config.log_to_file:
            # Create organized log directory structure
            log_dir = "logs/pipeline_logs"
            os.makedirs(log_dir, exist_ok=True)

            log_file_path = os.path.join(log_dir, self.config.log_file)
            file_handler = logging.FileHandler(log_file_path, encoding="utf-8")
            file_handler.setFormatter(detailed_formatter)
            file_handler.setLevel(logging.DEBUG)
            logger.addHandler(file_handler)

        return logger

    def get_logger(self) -> logging.Logger:
        """Get the configured logger instance."""
        return self.logger


class PipelineResults:
    """Track and manage pipeline execution results."""

    def __init__(self):
        self.start_time = datetime.now()
        self.end_time = None
        self.results = {}
        self.errors = []
        self.warnings = []

    def add_seller_result(
        self,
        seller_id: str,
        pipeline_type: str,
        success: bool,
        records_processed: int = 0,
        error_msg: str = None,
    ):
        """Add result for a specific seller and pipeline type."""
        if seller_id not in self.results:
            self.results[seller_id] = {}

        self.results[seller_id][pipeline_type] = {
            "success": success,
            "records_processed": records_processed,
            "error": error_msg,
        }

        if error_msg:
            self.errors.append(f"{seller_id} ({pipeline_type}): {error_msg}")

    def add_warning(self, message: str):
        """Add a warning message."""
        self.warnings.append(message)

    def finalize(self):
        """Finalize results and calculate summary statistics."""
        self.end_time = datetime.now()
        self.duration = (self.end_time - self.start_time).total_seconds()

    def get_summary(self) -> Dict[str, Any]:
        """Get summary statistics."""
        if not self.end_time:
            self.finalize()

        total_sellers = len(self.results)
        items_successful = sum(
            1 for r in self.results.values() if r.get("items", {}).get("success", False)
        )
        orders_successful = sum(
            1
            for r in self.results.values()
            if r.get("orders", {}).get("success", False)
        )
        fully_successful = sum(
            1
            for r in self.results.values()
            if all(pipeline.get("success", False) for pipeline in r.values())
        )

        return {
            "duration": self.duration,
            "total_sellers": total_sellers,
            "items_successful": items_successful,
            "orders_successful": orders_successful,
            "fully_successful": fully_successful,
            "total_errors": len(self.errors),
            "total_warnings": len(self.warnings),
        }

    def save_to_file(self, filepath: str = None):
        """Save results to JSON file in organized structure."""
        if filepath is None:
            # Create organized results directory and use timestamped filename
            results_dir = "logs/pipeline_results"
            os.makedirs(results_dir, exist_ok=True)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filepath = os.path.join(results_dir, f"pipeline_results_{timestamp}.json")
        else:
            # Ensure directory exists for custom filepath
            os.makedirs(os.path.dirname(filepath), exist_ok=True)

        data = {
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration": getattr(self, "duration", None),
            "results": self.results,
            "errors": self.errors,
            "warnings": self.warnings,
            "summary": self.get_summary(),
        }

        with open(filepath, "w") as f:
            json.dump(data, f, indent=2, default=str)


class ImprovedETLPipeline:
    """Enhanced ETL Pipeline with robust error handling and configuration."""

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.logger_setup = ImprovedPipelineLogger(config)
        self.logger = self.logger_setup.get_logger()
        self.results = PipelineResults()

    def validate_environment(self) -> bool:
        """Validate environment and API connectivity."""
        self.logger.info("Validating environment and API connectivity...")

        try:
            # Create data directory
            os.makedirs("./data", exist_ok=True)

            # Test API connection
            client, token = create_client()
            user_info = client.get_user(token)
            self.logger.info(
                f"✓ API connection successful. User: {user_info.get('nickname', 'Unknown')}"
            )

            # Validate seller IDs
            for seller_id in self.config.default_sellers:
                try:
                    # Quick test to validate seller exists
                    test_items = client.get_items(token, seller_id, limit=1)
                    self.logger.info(
                        f"✓ Seller {seller_id} validated ({len(test_items)} items available)"
                    )
                except Exception as e:
                    self.logger.warning(f"⚠ Seller {seller_id} validation failed: {e}")
                    self.results.add_warning(
                        f"Seller {seller_id} validation failed: {e}"
                    )

            return True

        except Exception as e:
            self.logger.error(f"❌ Environment validation failed: {e}")
            return False

    def run_items_pipeline(self, seller_id: str) -> bool:
        """Execute items ETL pipeline for a single seller with enhanced error handling."""
        self.logger.info(f"Starting ITEMS pipeline for seller {seller_id}")

        try:
            # Extract with enrichments
            self.logger.info("Extracting items with enrichments...")
            raw_items = extract_items_with_enrichments(
                seller_id=seller_id,
                limit=self.config.max_items_per_seller,
                include_descriptions=self.config.include_descriptions,
                include_reviews=self.config.include_reviews,
            )

            if not raw_items:
                self.logger.warning(f"No items extracted for seller {seller_id}")
                self.results.add_seller_result(
                    seller_id, "items", False, 0, "No items found"
                )
                return False

            self.logger.info(f"✓ Extracted {len(raw_items)} items")

            # Transform
            self.logger.info("Enriching items...")
            enriched_items = enrich_items(raw_items)

            if not enriched_items:
                error_msg = "Items enrichment failed - no items to load"
                self.logger.error(error_msg)
                self.results.add_seller_result(seller_id, "items", False, 0, error_msg)
                return False

            self.logger.info(f"✓ Enriched {len(enriched_items)} items")

            # Load
            self.logger.info("Loading items to database...")
            load_items_to_db(enriched_items, self.config.db_url)

            self.logger.info(
                f"✅ Items pipeline completed successfully for seller {seller_id}"
            )
            self.results.add_seller_result(
                seller_id, "items", True, len(enriched_items)
            )
            return True

        except Exception as e:
            error_msg = f"Items pipeline failed: {e}"
            self.logger.error(f"❌ {error_msg}")
            self.results.add_seller_result(seller_id, "items", False, 0, str(e))
            return False

    def run_orders_pipeline(self, seller_id: str) -> bool:
        """Execute orders ETL pipeline for a single seller with enhanced error handling."""
        self.logger.info(f"Starting ORDERS pipeline for seller {seller_id}")

        try:
            self.logger.info(
                f"Date range: {self.config.orders_date_from} to {self.config.orders_date_to}"
            )

            # Extract with proper pagination limit
            self.logger.info("Extracting orders...")
            raw_orders = extract_orders(
                seller_id=seller_id,
                date_from=self.config.orders_date_from,
                date_to=self.config.orders_date_to,
                limit=self.config.api_pagination_limit,  # Respect API limit
                max_records=self.config.max_orders_per_seller,
            )

            if not raw_orders:
                self.logger.warning(f"No orders extracted for seller {seller_id}")
                self.results.add_seller_result(
                    seller_id, "orders", False, 0, "No orders found"
                )
                return False

            self.logger.info(f"✓ Extracted {len(raw_orders)} orders")

            # Transform
            self.logger.info("Enriching orders...")
            enriched_orders = enrich_orders(raw_orders)

            if not enriched_orders:
                error_msg = "Orders enrichment failed - no orders to load"
                self.logger.error(error_msg)
                self.results.add_seller_result(seller_id, "orders", False, 0, error_msg)
                return False

            self.logger.info(f"✓ Enriched {len(enriched_orders)} orders")

            # Load
            self.logger.info("Loading orders to database...")
            load_orders_to_db(enriched_orders, self.config.db_url)

            self.logger.info(
                f"✅ Orders pipeline completed successfully for seller {seller_id}"
            )
            self.results.add_seller_result(
                seller_id, "orders", True, len(enriched_orders)
            )
            return True

        except Exception as e:
            error_msg = f"Orders pipeline failed: {e}"
            self.logger.error(f"❌ {error_msg}")
            self.results.add_seller_result(seller_id, "orders", False, 0, str(e))
            return False

    def run_full_pipeline(self, seller_id: str) -> Dict[str, bool]:
        """Execute both items and orders pipelines for a single seller."""
        self.logger.info(f"🚀 Starting FULL pipeline for seller {seller_id}")

        items_success = self.run_items_pipeline(seller_id)
        orders_success = self.run_orders_pipeline(seller_id)

        return {"items": items_success, "orders": orders_success}

    def run_multi_seller_pipeline(
        self, seller_ids: Optional[List[str]] = None
    ) -> Dict[str, Dict[str, bool]]:
        """Execute full pipeline for multiple sellers with comprehensive reporting."""
        if seller_ids is None:
            seller_ids = self.config.default_sellers

        self.logger.info(
            f"🎯 Starting MULTI-SELLER pipeline for {len(seller_ids)} sellers"
        )

        for i, seller_id in enumerate(seller_ids, 1):
            self.logger.info(f"📍 Processing seller {i}/{len(seller_ids)}: {seller_id}")
            try:
                self.run_full_pipeline(seller_id)
            except Exception as e:
                self.logger.error(f"❌ Critical failure for seller {seller_id}: {e}")
                self.results.add_seller_result(
                    seller_id, "items", False, 0, f"Critical failure: {e}"
                )
                self.results.add_seller_result(
                    seller_id, "orders", False, 0, f"Critical failure: {e}"
                )

        return self.results.results

    def generate_final_report(self):
        """Generate comprehensive final report."""
        self.results.finalize()
        summary = self.results.get_summary()

        self.logger.info("=" * 60)
        self.logger.info("📊 PIPELINE EXECUTION REPORT")
        self.logger.info("=" * 60)

        self.logger.info(f"⏱️  Total Duration: {summary['duration']:.2f} seconds")
        self.logger.info(f"🏪 Total Sellers: {summary['total_sellers']}")
        self.logger.info(
            f"📦 Items Successful: {summary['items_successful']}/{summary['total_sellers']}"
        )
        self.logger.info(
            f"🛒 Orders Successful: {summary['orders_successful']}/{summary['total_sellers']}"
        )
        self.logger.info(
            f"✅ Fully Successful: {summary['fully_successful']}/{summary['total_sellers']}"
        )

        if summary["total_errors"] > 0:
            self.logger.info(f"❌ Total Errors: {summary['total_errors']}")
            for error in self.results.errors:
                self.logger.error(f"   • {error}")

        if summary["total_warnings"] > 0:
            self.logger.info(f"⚠️  Total Warnings: {summary['total_warnings']}")
            for warning in self.results.warnings:
                self.logger.warning(f"   • {warning}")

        # Save detailed results with organized path
        self.results.save_to_file()  # Uses default organized path now

        # Get the actual filepath for logging
        results_dir = "logs/pipeline_results"
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results_file = f"pipeline_results_{timestamp}.json"
        self.logger.info(f"📄 Detailed results saved to: {results_dir}/{results_file}")

        # Overall status
        if (
            summary["fully_successful"] == summary["total_sellers"]
            and summary["total_errors"] == 0
        ):
            self.logger.info("🎉 PIPELINE EXECUTION: SUCCESS")
            return True
        else:
            self.logger.info("💥 PIPELINE EXECUTION: PARTIAL SUCCESS OR FAILURE")
            return False

    def save_current_config(self):
        """Save current pipeline configuration for future reference."""
        self.config.save_to_file()  # Uses default organized path
        self.logger.info(
            "💾 Pipeline configuration saved to: config/pipeline_configs.json"
        )


def load_config_from_args() -> PipelineConfig:
    """Load configuration from command line arguments or config file."""
    config = PipelineConfig()

    # Check for config file argument
    if len(sys.argv) > 1 and sys.argv[1].endswith(".json"):
        try:
            config = PipelineConfig.load_from_file(sys.argv[1])
            print(f"✓ Loaded configuration from {sys.argv[1]}")
        except Exception as e:
            print(f"⚠ Failed to load config file {sys.argv[1]}: {e}")
            print("Using default configuration...")

    return config


def main():
    """Enhanced main entry point with robust configuration and error handling."""
    print("🚀 Noneca.com Mercado Livre Analytics Pipeline")
    print("=" * 50)

    # Load configuration
    config = load_config_from_args()

    # Initialize pipeline
    pipeline = ImprovedETLPipeline(config)

    # Validate environment
    if not pipeline.validate_environment():
        print("❌ Environment validation failed. Exiting.")
        sys.exit(1)

    # Parse command line arguments for specific operations
    if len(sys.argv) > 1 and not sys.argv[1].endswith(".json"):
        seller_id = sys.argv[1]
        pipeline_type = sys.argv[2] if len(sys.argv) > 2 else "full"

        pipeline.logger.info(
            f"🎯 Running {pipeline_type.upper()} pipeline for seller: {seller_id}"
        )

        if pipeline_type == "items":
            success = pipeline.run_items_pipeline(seller_id)
        elif pipeline_type == "orders":
            success = pipeline.run_orders_pipeline(seller_id)
        else:  # "full" or any other value
            results = pipeline.run_full_pipeline(seller_id)
            success = all(results.values())

        pipeline.generate_final_report()
        sys.exit(0 if success else 1)

    # Multi-seller mode (default)
    pipeline.logger.info("🏪 Multi-seller mode: Processing all configured sellers")
    pipeline.run_multi_seller_pipeline()

    # Generate final report and exit
    success = pipeline.generate_final_report()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()

# --- .\config\config.py
#!/usr/bin/env python3
## config/config.py
"""Handles project configuration and environment variables."""
import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass
class Config:
    client_id: str = os.getenv("ML_CLIENT_ID")
    client_secret: str = os.getenv("ML_CLIENT_SECRET")
    redirect_uri: str = os.getenv("ML_REDIRECT_URI")
    timeout: int = int(os.getenv("API_TIMEOUT", 30))
    rate_limit: int = int(os.getenv("RATE_LIMIT", 100))

    # API URLs
    api_url: str = "https://api.mercadolibre.com"
    auth_url: str = "https://auth.mercadolivre.com.br"

    # Files
    token_file: str = "ml_tokens.json"

    # Fallback tokens
    fallback_access: str = os.getenv("ACCESS_TOKEN")
    fallback_refresh: str = os.getenv("REFRESH_TOKEN")
    fallback_expires: str = os.getenv("TOKEN_EXPIRES")


cfg = Config()

# --- .\src\extractors\items_extractor.py
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

# --- .\src\extractors\ml_api_client.py
#!/usr/bin/env python3
# src/extractors/ml_api_client.py
"""Mercado Livre API client with OAuth 2.0 integration and pagination support."""
import os
import json
import requests
import typing
from typing import Dict, Any, Optional
import secrets
from datetime import datetime, timedelta
from config.config import cfg


class MLClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(
            {"Accept": "application/json", "User-Agent": "MLExtractor/1.0"}
        )
        self._rate = {"calls": 0, "reset": datetime.now()}

    def _check_rate(self):
        now = datetime.now()
        if now - self._rate["reset"] > timedelta(minutes=1):
            self._rate = {"calls": 0, "reset": now}
        if self._rate["calls"] >= cfg.rate_limit:
            raise Exception("Rate limit exceeded")
        self._rate["calls"] += 1

    def _req(self, method, endpoint, **kwargs):
        self._check_rate()
        url = f"{cfg.api_url}{endpoint}"

        kwargs.setdefault("headers", {}).update(
            {"X-Request-ID": secrets.token_hex(8), "Cache-Control": "no-cache"}
        )

        try:
            resp = self.session.request(method, url, timeout=cfg.timeout, **kwargs)
            resp.raise_for_status()
            return {} if resp.status_code == 204 else resp.json()
        except requests.exceptions.HTTPError:
            status = resp.status_code
            try:
                err = resp.json()
            except ValueError:
                err = {"error": "Invalid JSON"}

            error_map = {
                401: f"Unauthorized: {err}",
                403: f"Forbidden: {err}",
                404: f"Not found: {endpoint}",
                429: "Rate limited",
            }
            raise Exception(error_map.get(status, f"HTTP {status}: {err}"))
        except requests.exceptions.Timeout:
            raise Exception("Request timeout")
        except Exception as e:
            raise Exception(f"Request failed: {str(e)}")

    def _auth(self, token):
        self.session.headers.update({"Authorization": f"Bearer {token}"})

    # Core API methods
    def get_user(self, token, user_id="me", attrs=None):
        self._auth(token)
        params = {"attributes": attrs} if attrs else {}
        return self._req("GET", f"/users/{user_id}", params=params)

    def get_items(self, token, seller_id, limit=None, status="active"):
        """
        Extract items for a seller with pagination support.

        Args:
            token: Authentication token
            seller_id: The seller ID to extract items for
            limit: Maximum number of items to extract (None for all items)
            status: Item status filter (default: "active")

        Returns:
            List of item dictionaries with full details
        """
        self._auth(token)

        collected_items = []
        offset = 0
        page_size = 100  # Maximum items per API request

        while True:
            # Calculate how many items to request this round
            if limit is not None:
                remaining = limit - len(collected_items)
                if remaining <= 0:
                    break
                current_limit = min(page_size, remaining)
            else:
                current_limit = page_size

            params = {"limit": current_limit, "offset": offset, "status": status}

            try:
                result = self._req(
                    "GET", f"/users/{seller_id}/items/search", params=params
                )
                batch_ids = result.get("results", [])

                if not batch_ids:
                    break  # No more items

                # Fetch full item details for this batch
                batch_items = []
                for item_id in batch_ids:
                    try:
                        item_details = self.get_item(token, item_id)
                        if item_details:
                            batch_items.append(item_details)
                    except Exception as e:
                        print(f"Warning: Failed to get details for item {item_id}: {e}")
                        continue

                collected_items.extend(batch_items)
                offset += len(batch_ids)

                # Stop if we got fewer items than requested (end of data)
                if len(batch_ids) < current_limit:
                    break

            except Exception as e:
                print(f"Error fetching items batch at offset {offset}: {e}")
                break

        return collected_items[:limit] if limit else collected_items

    def get_item(self, token, item_id, attrs=None):
        self._auth(token)
        params = {"attributes": attrs} if attrs else {}
        return self._req("GET", f"/items/{item_id}", params=params)

    def get_desc(self, token, item_id):
        self._auth(token)
        try:
            return self._req("GET", f"/items/{item_id}/description")
        except Exception as e:
            return {"plain_text": "N/A", "error": str(e)}

    def get_reviews(self, token, item_id):
        self._auth(token)
        try:
            return self._req("GET", f"/reviews/item/{item_id}")
        except Exception as e:
            return {
                "rating_average": 0,
                "total_reviews": 0,
                "reviews": [],
                "error": str(e),
            }

    def get_questions(self, token, item_id, limit=10):
        self._auth(token)
        try:
            params = {"item_id": item_id, "limit": limit}
            return self._req("GET", "/questions/search", params=params)
        except Exception as e:
            return {"questions": [], "total": 0, "error": str(e)}

    def get_orders(
        self,
        token: str,
        seller_id: str,
        *,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        sort: str = "date_created",
        limit: int = 100,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """
        Hits /orders/search with full filter/sort/pagination support.
        Returns the raw JSON (including `results`, `paging`, etc.) for callers to
        page through as needed.
        """
        # Ensure we have a valid bearer token on the session
        self._auth(token)

        # Build query parameters
        params: Dict[str, Any] = {
            "seller": seller_id,
            "limit": limit,
            "offset": offset,
            "sort": "date_asc" if sort == "date_created" else sort,
        }
        if date_from:
            params["order.date_created.from"] = date_from
        if date_to:
            params["order.date_created.to"] = date_to

        # Delegate to the shared request method
        response = self._req("GET", "/orders/search", params=params)

        # response is expected to include both "results" and "paging"
        return response

    def get_listing_types(self, token, site_id):
        self._auth(token)
        return self._req("GET", f"/sites/{site_id}/listing_types")

    def get_listing_exposures(self, token, site_id):
        self._auth(token)
        return self._req("GET", f"/sites/{site_id}/listing_exposures")

    def search(
        self,
        token,
        site_id,
        query=None,
        seller_id=None,
        category=None,
        limit=50,
        offset=0,
    ):
        # Authenticate immediately
        self._auth(token)

        # Always build the same param dictionary
        params = {"limit": limit, "offset": offset}
        if query:
            params["q"] = query
        if seller_id:
            params["seller_id"] = seller_id
        if category:
            params["category"] = category

        # 1) Try the generic /sites/{site_id}/search
        try:
            return self._req("GET", f"/sites/{site_id}/search", params=params)
        except Exception as e:
            # If it fails with 403 or 401, fall back to "items by seller" if we can
            msg = str(e).lower()
            if "403" in msg or "401" in msg:
                # If caller already passed seller_id, just re‐raise (nothing else to try)
                if seller_id:
                    raise

                # Otherwise, fetch the user's own ID and do /users/{id}/items/search
                user = self.get_user(token)  # returns JSON with "id" field
                fallback_seller = user["id"]
                return self.get_items(token, fallback_seller, limit=limit)
            # For any other exception, re‐raise
            raise

    def get_categories(self, token, site_id):
        try:
            return self._req("GET", f"/sites/{site_id}/categories")
        except Exception:
            return self._req("GET", f"/sites/{site_id}/categories")

    def get_category(self, token, category_id):
        try:
            return self._req("GET", f"/categories/{category_id}")
        except Exception:
            return self._req("GET", f"/categories/{category_id}")

    def get_trends(self, token, site_id, category_id=None):
        endpoint = f"/trends/{site_id}"
        if category_id:
            endpoint += f"/{category_id}"
        return self._req("GET", endpoint, headers={"Authorization": f"Bearer {token}"})

    def validate_item(self, token, item_data):
        self._auth(token)
        try:
            resp = self.session.post(
                f"{cfg.api_url}/items/validate",
                json=item_data,
                headers=self.session.headers,
                timeout=cfg.timeout,
            )
            return {
                "valid": resp.status_code == 204,
                "errors": resp.json() if resp.status_code != 204 else None,
            }
        except Exception as e:
            return {"valid": False, "errors": str(e)}


# Token management
def save_tokens(tokens):
    tokens["expires_at"] = (
        datetime.now() + timedelta(seconds=tokens["expires_in"])
    ).isoformat()
    with open(cfg.token_file, "w") as f:
        json.dump(tokens, f)


def load_tokens():
    if os.path.exists(cfg.token_file):
        with open(cfg.token_file) as f:
            return json.load(f)
    return {
        "access_token": cfg.fallback_access,
        "token_type": "Bearer",
        "expires_in": 21600,
        "refresh_token": cfg.fallback_refresh,
        "expires_at": cfg.fallback_expires,
    }


def is_valid(tokens):
    if not tokens:
        return False
    expires_at = datetime.fromisoformat(tokens["expires_at"])
    return datetime.now() < expires_at - timedelta(minutes=5)


def refresh_token(refresh_token):
    resp = requests.post(
        f"{cfg.api_url}/oauth/token",
        data={
            "grant_type": "refresh_token",
            "client_id": cfg.client_id,
            "client_secret": cfg.client_secret,
            "refresh_token": refresh_token,
        },
        timeout=cfg.timeout,
    )
    resp.raise_for_status()
    return resp.json()


def get_token():
    tokens = load_tokens()

    if tokens and is_valid(tokens):
        return tokens["access_token"]

    if tokens and "refresh_token" in tokens:
        try:
            new_tokens = refresh_token(tokens["refresh_token"])
            save_tokens(new_tokens)
            return new_tokens["access_token"]
        except Exception:
            pass

    return load_tokens()["access_token"]


def create_client():
    return MLClient(), get_token()

# --- .\src\extractors\orders_extractor.py
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

# --- .\src\loaders\data_loader.py
# src/loaders/data_loader.py
"""Generic loader for persisting product data to database."""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
import logging

from src.models.models import Item, PriceHistory, Seller, create_all_tables
from src.models.models import Order, Buyer, OrderItem  # Add to the imports

logger = logging.getLogger(__name__)


def load_items_to_db(enriched_items, db_url="sqlite:///./data/noneca_analytics.db"):
    """
    Upsert a list of enriched item dicts into `items` and append to `price_history`.
    If seller info is embedded, upsert into `sellers` as well.
    """
    if not enriched_items:
        logger.info("No items to load")
        return

    engine = create_engine(db_url, echo=False, future=True)
    Session = sessionmaker(bind=engine)
    create_all_tables(engine)

    session = Session()
    try:
        for record in enriched_items:
            item_id = record.get("item_id")
            if not item_id:
                continue  # skip invalid entries

            # Upsert item
            existing = session.get(Item, item_id)
            if existing:
                # Update only the mutable fields
                for field in (
                    "title",
                    "category_id",
                    "current_price",
                    "original_price",
                    "available_quantity",
                    "sold_quantity",
                    "condition",
                    "brand",
                    "size",
                    "color",
                    "gender",
                    "views",
                    "conversion_rate",
                    "seller_id",
                    "updated_at",
                ):
                    if field in record:
                        setattr(existing, field, record[field])
                session.add(existing)
            else:
                item_kwargs = {k: record[k] for k in record.keys() if hasattr(Item, k)}
                session.add(Item(**item_kwargs))

            # Optionally upsert seller if detailed info present
            seller_info = {
                "seller_id": record.get("seller_id"),
                "nickname": record.get("seller_nickname"),
                "reputation_score": record.get("seller_reputation"),
                "transactions_completed": record.get("seller_transactions"),
                "is_competitor": record.get("is_competitor"),
                "market_share_pct": record.get("market_share_pct"),
            }
            sid = seller_info.get("seller_id")
            if sid and any(v is not None for v in seller_info.values()):
                existing_seller = session.get(Seller, sid)
                if existing_seller:
                    for key, val in seller_info.items():
                        if key != "seller_id" and val is not None:
                            setattr(existing_seller, key, val)
                    session.add(existing_seller)
                else:
                    session.add(Seller(**seller_info))

            # Append price history snapshot
            price_record = {
                "item_id": item_id,
                "price": record.get("current_price"),
                "discount_percentage": record.get("discount_percentage"),
                "competitor_rank": record.get("competitor_rank"),
                "price_position": record.get("price_position"),
            }
            session.add(PriceHistory(**price_record))

        session.commit()
        logger.info(f"Successfully loaded {len(enriched_items)} items to database")
    except SQLAlchemyError as e:
        session.rollback()
        logger.error(f"Failed to load items to database: {e}")
        raise
    finally:
        session.close()


def load_orders_to_db(enriched_orders, db_url="sqlite:///./data/noneca_analytics.db"):
    """
    Upsert enriched order data into Buyers, Sellers, Orders, and OrderItems tables.

    FIXED: Properly handle dictionary data and create ORM instances instead of
    passing raw dictionaries to SQLAlchemy operations.
    """
    if not enriched_orders:
        logger.info("No orders to load")
        return

    engine = create_engine(db_url, echo=False, future=True)
    Session = sessionmaker(bind=engine)
    create_all_tables(engine)

    session = Session()
    try:
        orders_loaded = 0
        buyers_loaded = 0
        sellers_loaded = 0
        order_items_loaded = 0

        for record in enriched_orders:
            try:
                # --- Upsert Buyer ---
                buyer_id = record.get("buyer_id")
                if buyer_id:
                    existing_buyer = session.get(Buyer, buyer_id)
                    if existing_buyer:
                        if record.get("buyer_nickname"):
                            existing_buyer.nickname = record["buyer_nickname"]
                    else:
                        # Create new Buyer instance properly
                        new_buyer = Buyer(
                            buyer_id=buyer_id, nickname=record.get("buyer_nickname")
                        )
                        session.add(new_buyer)
                        buyers_loaded += 1

                # --- Upsert Seller ---
                seller_id = record.get("seller_id")
                seller_nickname = record.get("seller_nickname")
                if seller_id:
                    existing_seller = session.get(Seller, seller_id)
                    if existing_seller:
                        if seller_nickname:
                            existing_seller.nickname = seller_nickname
                    else:
                        # Create new Seller instance properly
                        new_seller = Seller(
                            seller_id=seller_id, nickname=seller_nickname
                        )
                        session.add(new_seller)
                        sellers_loaded += 1

                # --- Upsert Order ---
                order_id = record.get("order_id")
                if not order_id:
                    logger.warning(f"Skipping order with missing order_id: {record}")
                    continue

                existing_order = session.get(Order, order_id)
                if existing_order:
                    # Update existing order fields
                    for field in (
                        "status",
                        "total_amount",
                        "total_fees",
                        "profit_margin",
                        "currency_id",
                        "seller_id",
                        "buyer_id",
                        "date_created",
                        "date_closed",
                    ):
                        if field in record and record[field] is not None:
                            setattr(existing_order, field, record[field])
                else:
                    # Create new Order instance with only valid fields
                    order_fields = {}
                    for field in (
                        "order_id",
                        "status",
                        "total_amount",
                        "total_fees",
                        "profit_margin",
                        "currency_id",
                        "seller_id",
                        "buyer_id",
                        "date_created",
                        "date_closed",
                    ):
                        if field in record and record[field] is not None:
                            order_fields[field] = record[field]

                    # Create the Order instance properly
                    new_order = Order(**order_fields)
                    session.add(new_order)
                    orders_loaded += 1

                # --- Insert OrderItems ---
                items = record.get("items", [])
                for item_data in items:
                    # Only create OrderItem if we have required fields
                    if item_data.get("item_id"):
                        order_item_fields = {}
                        for field in (
                            "order_id",
                            "item_id",
                            "quantity",
                            "unit_price",
                            "sale_fee",
                            "listing_type",
                            "variation_id",
                        ):
                            if field == "order_id":
                                order_item_fields[field] = order_id
                            elif field in item_data and item_data[field] is not None:
                                order_item_fields[field] = item_data[field]

                        # Create OrderItem instance properly
                        new_order_item = OrderItem(**order_item_fields)
                        session.add(new_order_item)
                        order_items_loaded += 1

            except Exception as e:
                logger.error(
                    f"Error processing order {record.get('order_id', 'unknown')}: {e}"
                )
                continue

        # Commit all changes
        session.commit()
        logger.info(f"Successfully loaded to database:")
        logger.info(f"  - Orders: {orders_loaded}")
        logger.info(f"  - Buyers: {buyers_loaded}")
        logger.info(f"  - Sellers: {sellers_loaded}")
        logger.info(f"  - Order Items: {order_items_loaded}")

    except SQLAlchemyError as e:
        session.rollback()
        logger.error(f"Database error during order loading: {e}")
        raise
    except Exception as e:
        session.rollback()
        logger.error(f"Unexpected error during order loading: {e}")
        raise
    finally:
        session.close()

# --- .\src\models\models.py
# models/models.py
"""
SQLAlchemy models for products, sellers, and transactional order data,
designed with a star schema approach for business intelligence analytics.
"""
from sqlalchemy import (
    Column,
    String,
    Integer,
    Float,
    Boolean,
    Date,
    DateTime,
    func,
    text,
    ForeignKey,
)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class Item(Base):
    __tablename__ = "items"

    item_id = Column(String(50), primary_key=True)
    title = Column(String(500))
    category_id = Column(String(50), index=True)
    current_price = Column(Float(precision=2))
    original_price = Column(Float(precision=2))
    available_quantity = Column(Integer)
    sold_quantity = Column(Integer)
    condition = Column(String(20))
    brand = Column(String(100), index=True)
    size = Column(String(20))
    color = Column(String(50))
    gender = Column(String(20))
    views = Column(Integer, default=0)
    conversion_rate = Column(Float(precision=4))
    seller_id = Column(Integer, ForeignKey("sellers.seller_id"), index=True)
    created_at = Column(DateTime, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(
        DateTime,
        server_default=text("CURRENT_TIMESTAMP"),
        onupdate=func.current_timestamp,
    )

    # Relationships to link to other tables
    seller = relationship("Seller", back_populates="items")
    order_items = relationship("OrderItem", back_populates="item")
    price_history = relationship(
        "PriceHistory", back_populates="item", cascade="all, delete-orphan"
    )


class PriceHistory(Base):
    __tablename__ = "price_history"

    id = Column(Integer, primary_key=True, autoincrement=True)
    item_id = Column(String(50), ForeignKey("items.item_id"), index=True)
    price = Column(Float(precision=2))
    discount_percentage = Column(Float(precision=2))
    competitor_rank = Column(Integer, nullable=True)
    price_position = Column(String(20), nullable=True)
    recorded_at = Column(DateTime, server_default=text("CURRENT_TIMESTAMP"), index=True)

    # Relationship back to the Item
    item = relationship("Item", back_populates="price_history")


class Seller(Base):
    __tablename__ = "sellers"

    seller_id = Column(Integer, primary_key=True)
    nickname = Column(String(100), nullable=True)
    reputation_score = Column(Float(precision=2), nullable=True)
    transactions_completed = Column(Integer, nullable=True)
    is_competitor = Column(Boolean, default=False)
    market_share_pct = Column(Float(precision=2), nullable=True)

    # Relationships to see all items and orders from a seller
    items = relationship("Item", back_populates="seller")
    orders = relationship("Order", back_populates="seller")


class MarketTrend(Base):
    __tablename__ = "market_trends"

    id = Column(Integer, primary_key=True, autoincrement=True)
    keyword = Column(String(200))
    search_volume = Column(Integer)
    category_id = Column(String(50), index=True)
    trend_date = Column(Date)
    growth_rate = Column(Float(precision=2))


# --- New Models for Orders and Buyers ---


class Buyer(Base):
    """Dimension table for customer/buyer information."""

    __tablename__ = "buyers"

    buyer_id = Column(Integer, primary_key=True)
    nickname = Column(String(100), nullable=True)

    # Relationship to see all orders from a buyer
    orders = relationship("Order", back_populates="buyer")


class Order(Base):
    """Fact table for order headers, linking buyers and sellers."""

    __tablename__ = "orders"

    order_id = Column(Integer, primary_key=True)
    status = Column(String(50), index=True)
    total_amount = Column(Float(precision=2))
    total_fees = Column(Float(precision=2))
    profit_margin = Column(Float(precision=2))
    currency_id = Column(String(10))
    date_created = Column(DateTime(timezone=True), index=True)
    date_closed = Column(DateTime(timezone=True))

    # Foreign Keys to link to dimension tables
    seller_id = Column(Integer, ForeignKey("sellers.seller_id"), index=True)
    buyer_id = Column(Integer, ForeignKey("buyers.buyer_id"), index=True)

    # SQLAlchemy Relationships
    seller = relationship("Seller", back_populates="orders")
    buyer = relationship("Buyer", back_populates="orders")
    items = relationship(
        "OrderItem", back_populates="order", cascade="all, delete-orphan"
    )


class OrderItem(Base):
    """Fact table for order line items, linking orders to specific products."""

    __tablename__ = "order_items"

    order_item_id = Column(Integer, primary_key=True, autoincrement=True)
    quantity = Column(Integer)
    unit_price = Column(Float(precision=2))  # Price at the time of sale
    sale_fee = Column(Float(precision=2))
    listing_type = Column(String(50))
    variation_id = Column(Integer)  # For tracking specific variations (color/size)

    # Foreign Keys to link facts and dimensions
    order_id = Column(Integer, ForeignKey("orders.order_id"), index=True)
    item_id = Column(String(50), ForeignKey("items.item_id"), index=True)

    # SQLAlchemy Relationships
    order = relationship("Order", back_populates="items")
    item = relationship("Item", back_populates="order_items")


def create_all_tables(engine):
    """Create all tables in the target database."""
    Base.metadata.create_all(engine)

# --- .\src\transformers\order_enricher.py
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

# --- .\src\transformers\product_enricher.py
# src/transformers/product_enricher.py
"""Applies enrichment logic to product catalog data."""
from datetime import datetime, timezone
from typing import List, Dict, Optional, Any


def _get_attr(attrs: Optional[List[Dict]], key: str) -> Optional[str]:
    """Extract attribute value by key from attributes list."""
    if not attrs:
        return None

    for attr in attrs:
        if attr.get("id") == key:
            value = attr.get("value_name") or attr.get("value_id")
            # Return None if value is empty string or None
            return value if value else None
    return None


def _safe_divide(numerator: float, denominator: float, precision: int = 4) -> float:
    """Safely divide two numbers, returning 0.0 if denominator is 0."""
    if not denominator:
        return 0.0
    return round(numerator / denominator, precision)


def _calculate_discount_percentage(original: float, current: float) -> float:
    """Calculate discount percentage between original and current price."""
    if not original or original <= current:
        return 0.0
    return round((original - current) / original * 100, 2)


def enrich_item(item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Enrich a single item with computed fields and standardized format.

    Args:
        item: Raw item dictionary from API

    Returns:
        Enriched item dictionary
    """
    if not item:
        return {}

    attrs = item.get("attributes", [])
    seller_info = item.get("seller", {})  # Extract the full seller object

    # Extract attributes - note the correct attribute key for color
    brand = _get_attr(attrs, "BRAND")
    size = _get_attr(attrs, "SIZE")
    color = _get_attr(attrs, "MAIN_COLOR")  # Fixed: was "COLOR", should be "MAIN_COLOR"
    gender = _get_attr(attrs, "GENDER")

    # Calculate metrics
    views = item.get("views", 0) or 0
    sold = item.get("sold_quantity", 0) or 0
    conversion = _safe_divide(sold, views)

    current_price = float(item.get("price", 0) or 0)
    original_price = float(item.get("original_price") or current_price)
    discount_pct = _calculate_discount_percentage(original_price, current_price)

    timestamp = datetime.now(timezone.utc)

    return {
        "item_id": item.get("id"),
        "title": item.get("title"),
        "category_id": item.get("category_id"),
        "current_price": current_price,
        "original_price": original_price,
        "available_quantity": item.get("available_quantity"),
        "sold_quantity": sold,
        "condition": item.get("condition"),
        "brand": brand,
        "size": size,
        "color": color,
        "gender": gender,
        "views": views,
        "conversion_rate": conversion,
        "seller_id": item.get("seller_id"),
        # Add seller_nickname for relational Seller table
        "seller_nickname": seller_info.get("nickname"),
        "created_at": timestamp,
        "updated_at": timestamp,
        "discount_percentage": discount_pct,
    }


def enrich_items(raw_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Enrich a list of items with computed fields and standardized format.

    Args:
        raw_items: List of raw item dictionaries from API

    Returns:
        List of enriched item dictionaries
    """
    if not raw_items:
        return []

    return [enrich_item(item) for item in raw_items if item]

```