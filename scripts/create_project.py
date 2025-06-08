import os

PROJECT_ROOT = "noneca_com_main_v2"

# File definitions with one-liner docstrings
files = {
    "main.py": '# main.py\n"""Main script to run the product ETL pipeline."""',
    "scripts/fetch_orders_cli.py": '# fetch_orders_cli.py\n"""Interactive CLI for extracting order data from Mercado Livre API."""',
    "config/config.py": '# config.py\n"""Handles project configuration and environment variables."""',
    "src/models/models.py": '# models.py\n"""SQLAlchemy models for products and sellers."""',
    "src/models/order_models.py": '# order_models.py\n"""SQLAlchemy models specific to orders and transactions."""',
    "src/extractors/ml_api_client.py": '# ml_api_client.py\n"""Mercado Livre API client with OAuth 2.0 integration."""',
    "src/extractors/items_extractor.py": '# items_extractor.py\n"""Extractor for fetching product catalog data."""',
    "src/extractors/orders_extractor.py": '# orders_extractor.py\n"""Service to extract order data using Mercado Livre API."""',
    "src/transformers/product_enricher.py": '# product_enricher.py\n"""Applies enrichment logic to product catalog data."""',
    "src/transformers/order_enricher.py": '# order_enricher.py\n"""Transforms and enriches order transaction data."""',
    "src/loaders/data_loader.py": '# data_loader.py\n"""Generic loader for persisting product data to database."""',
    "src/loaders/order_loader.py": '# order_loader.py\n"""Loader module for order transaction data."""',
    "src/services/etl_orchestrator.py": '# etl_orchestrator.py\n"""Coordinates unified ETL execution for product and order data."""',
    "src/services/sales_analytics.py": '# sales_analytics.py\n"""Performs sales performance and revenue analytics."""',
    "src/services/market_service.py": '# market_service.py\n"""Provides competitor and market share analysis tools."""',
    "src/services/pricing_service.py": '# pricing_service.py\n"""Analyzes pricing effectiveness and price optimization."""',
    "src/services/forecast_service.py": '# forecast_service.py\n"""Implements forecasting logic for revenue and inventory."""',
    "src/utils/date_utils.py": '# date_utils.py\n"""Utilities for timezone and datetime manipulation."""',
    "src/utils/correlation_engine.py": '# correlation_engine.py\n"""Correlates product metrics with order and sales performance."""',
    "dashboard/pages/products_dashboard.py": '# products_dashboard.py\n"""Visualizations for product catalog performance."""',
    "dashboard/pages/sales_dashboard.py": '# sales_dashboard.py\n"""Dashboard for sales performance analytics."""',
    "dashboard/pages/unified_dashboard.py": '# unified_dashboard.py\n"""Combined dashboard for product and sales intelligence."""',
    "dashboard/components/product_components.py": '# product_components.py\n"""Reusable UI components for product visualizations."""',
    "dashboard/components/sales_components.py": '# sales_components.py\n"""UI components for displaying sales insights."""',
    "dashboard/layout.py": '# layout.py\n"""Defines the layout for the unified analytics dashboard."""',
    "tests/test_items_extractor.py": '# test_items_extractor.py\n"""Test suite for product catalog extraction."""',
    "tests/test_orders_extractor.py": '# test_orders_extractor.py\n"""Tests for order data extraction functionality."""',
    "tests/test_sales_analytics.py": '# test_sales_analytics.py\n"""Tests for the sales analytics engine."""',
    "tests/test_unified_etl.py": '# test_unified_etl.py\n"""Integration tests for the unified ETL pipeline."""',
    "requirements.txt": '# requirements.txt\n"""Project dependencies."""',
    ".env": '# .env\n"""Environment variables (do not commit this file)."""',
    "ml_tokens.json": '# ml_tokens.json\n"""OAuth token storage for Mercado Livre API."""',
    "README.md": '# README.md\n"""Documentation for Mercado Livre Analytics Platform."""',
}

# Ensure __init__.py in every applicable module directory
module_dirs = [
    "scripts",
    "config",
    "src",
    "src/models",
    "src/extractors",
    "src/transformers",
    "src/loaders",
    "src/services",
    "src/utils",
    "dashboard",
    "dashboard/pages",
    "dashboard/components",
    "tests",
]


def create_file(path, content):
    with open(path, "w", encoding="utf-8") as f:
        f.write(content + "\n")


def build_project():
    os.makedirs(PROJECT_ROOT, exist_ok=True)
    os.chdir(PROJECT_ROOT)

    # Create folders and __init__.py files
    for module_dir in module_dirs:
        os.makedirs(module_dir, exist_ok=True)
        init_path = os.path.join(module_dir, "__init__.py")
        if not os.path.exists(init_path):
            create_file(
                init_path, f'# __init__.py\n"""Initialize {module_dir} module."""'
            )

    # Create files with headers
    for file_path, header in files.items():
        full_path = os.path.join(PROJECT_ROOT, file_path)
        dir_name = os.path.dirname(full_path)
        if not os.path.exists(dir_name):
            os.makedirs(dir_name, exist_ok=True)
        create_file(file_path, header)

    # Create data/ folder and .gitkeep inside it
    os.makedirs("data", exist_ok=True)
    create_file("data/.gitkeep", '# .gitkeep\n"""Ensure data folder is versioned."""')

    print("✅ Project skeleton created successfully.")


if __name__ == "__main__":
    build_project()
