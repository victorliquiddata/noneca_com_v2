**[![Python](https://img.shields.io/badge/python-3.9%2B-blue)](https://www.python.org/)**
**[![Tests](https://img.shields.io/badge/tests-84%2F84-green)](https://github.com/victorliquiddata/noneca_com_v2/actions)**
**[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)**

# Mercado Livre Analytics Platform

Comprehensive ETL Pipeline & Business Intelligence for **Noneca.com**

**Production-ready e-commerce analytics platform** integrating with Mercado Livre Brasil’s API for marketplace intelligence. Built for intimate apparel retailer **noneca.com**, this project demonstrates end-to-end data engineering: dual ETL pipelines, interactive CLI, and automated insights.

---

## 📖 Table of Contents

1. [Project Overview](#project-overview)
2. [Key Features](#key-features)
3. [Architecture](#architecture)
4. [Getting Started](#getting-started)
5. [Documentation](#documentation)
6. [License](#license)

---

## Project Overview

* **Marketplace**: Mercado Livre Brasil (MLB)
* **Focus**: Intimate apparel analytics & BI
* **Data Sources**: Product Catalog & Order Transactions
* **Storage**: SQLite + SQLAlchemy ORM
* **Interface**: CLI & (future) Web Dashboard

---

## Key Features

* **Dual ETL Pipelines**: Separate workflows for products and orders
* **Interactive CLI**: Flexible extraction strategies (last 100, 500, 5000 orders)
* **OAuth 2.0**: Secure ML API integration with token management
* **Automated Enrichment**: Conversion rates, discounts, and sales metrics
* **Comprehensive Testing**: 84/84 tests passing with pytest

---

## Architecture

<details>
<summary><strong>🔧 System Architecture (click to expand)</strong></summary>

```mermaid
flowchart TD
    CLI[Command Line Interface] <--> ProductETL[Product ETL Pipeline]
    ProductETL --> DB1[SQLite Products]
    OrdersCLI[Orders CLI] <--> OrdersETL[Orders ETL Pipeline]
    OrdersETL --> DB2[SQLite Orders]
    ProductETL <--> API1[Mercado Livre Items API]
    OrdersETL <--> API2[Mercado Livre Orders API]
```

</details>

---

## Getting Started

1. **Clone the repo**

   ```bash
   git clone https://github.com/victorliquiddata/noneca_com_v2.git
   cd noneca_com_v2
   ```
2. **Set up environment**

   ```bash
   python -m venv venv
   source venv/bin/activate  # or `venv\Scripts\activate` on Windows
   pip install -r requirements.txt
   ```
3. **Configure credentials**
   Create a `.env` file:

   ```ini
   ML_CLIENT_ID=your_client_id
   ML_CLIENT_SECRET=your_client_secret
   ACCESS_TOKEN=your_fallback_token
   REFRESH_TOKEN=your_refresh_token
   ```
4. **Run ETL**

   ```bash
   # Products pipeline
   python main.py

   # Orders pipeline (interactive)
   python scripts/fetch_orders_cli.py
   ```

---

## Documentation

* **ETL Architecture**: `docs/etl_architecture.md`
* **Database Schema**: `docs/database_schema.md`
* **Usage Guide**: `docs/usage_guide.md`
* **Development Log**: `docs/development_log.md`

---

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
