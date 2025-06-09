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
