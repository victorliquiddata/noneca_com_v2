# Usage Guide

## Quick Start

### Prerequisites
- Python 3.9+
- Mercado Livre API credentials
- Git for version control

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/victorliquiddata/noneca_com_v2.git
   cd noneca_com_v2
   ```

2. **Set up virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   # or
   venv\Scripts\activate     # Windows
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment**
   Create a `.env` file in the project root:
   ```ini
    # MercadoLibre API Configuration
    ML_CLIENT_ID=your_id
    ML_CLIENT_SECRET=your_secret
    ML_REDIRECT_URI=your_uri

    # API Configuration
    API_TIMEOUT=30
    RATE_LIMIT=100

    # Fallback Tokens
    ACCESS_TOKEN=your_token
    REFRESH_TOKEN=your_refresh
    TOKEN_EXPIRES=your_token_exp
   ```

## Core Operations

### Product ETL Pipeline

#### Basic Usage
```bash
# Run product extraction for default seller
python main.py

# Run product extraction for specific seller
python main.py 354140329
```

#### Advanced Configuration
```python
# Custom category extraction
from src.extractors.items_extractor import ItemsExtractor

extractor = ItemsExtractor()
items = extractor.extract_items_by_category("MLB1576", limit=100)
```

### Orders ETL Pipeline

#### Interactive CLI
```bash
# Launch interactive orders extraction
python scripts/fetch_orders_cli.py
```

**Available Options:**
1. **Last 500 orders** (recommended for initial analysis)
2. **Last 5000 orders** (comprehensive historical analysis)
3. **All available orders** (complete 12-month dataset)
4. **Recent 100 orders** (quick status check)

#### Programmatic Usage
```python
from src.extractors.orders_extractor import OrdersExtractor
from datetime import datetime, timedelta

# Initialize extractor
extractor = OrdersExtractor()

# Extract orders from last 30 days
end_date = datetime.now()
start_date = end_date - timedelta(days=30)

orders = extractor.extract_orders_by_date_range(
    seller_id=354140329,
    start_date=start_date,
    end_date=end_date
)
```

## Configuration Options

### API Configuration
```python
# config/config.py
ML_API_BASE_URL = "https://api.mercadolibre.com"
ML_AUTH_URL = "https://auth.mercadolibre.com.br"
DEFAULT_SELLER_ID = 354140329
DEFAULT_CATEGORY = "MLB1576"  # Intimate apparel
```

### Database Configuration
```python
# SQLite database settings
DATABASE_URL = "sqlite:///data/noneca_analytics.db"
ECHO_SQL = False  # Set to True for SQL debugging
```

### Extraction Parameters
```python
# Pagination settings
ORDERS_PER_PAGE = 50
MAX_RETRIES = 3
RATE_LIMIT_DELAY = 1.0  # seconds

# Date range settings
DEFAULT_DAYS_BACK = 365  # 12 months
TIMEZONE = "America/Sao_Paulo"
```

## Data Analysis Workflows

### Basic Product Analysis
```python
from src.models.models import SessionLocal, Item
from sqlalchemy import func

# Connect to database
session = SessionLocal()

# Get top-selling products
top_products = session.query(Item)\
    .order_by(Item.sold_quantity.desc())\
    .limit(10)\
    .all()

# Calculate average conversion rate
avg_conversion = session.query(func.avg(Item.conversion_rate)).scalar()

session.close()
```

### Sales Performance Analysis
```python
from src.models.models import SessionLocal, Order, OrderItem
from datetime import datetime, timedelta

session = SessionLocal()

# Get recent orders
recent_orders = session.query(Order)\
    .filter(Order.date_created >= datetime.now() - timedelta(days=30))\
    .all()

# Calculate total revenue
total_revenue = sum(order.total_amount for order in recent_orders)

session.close()
```

## CLI Commands Reference

### Product Pipeline Commands
```bash
# Extract products for default seller
python main.py

# Extract products for specific seller
python main.py [SELLER_ID]

# Extract with verbose logging
python main.py --verbose

# Extract specific category
python main.py --category MLB1576
```

### Orders Pipeline Commands
```bash
# Interactive orders extraction
python scripts/fetch_orders_cli.py

# Extract specific number of orders
python scripts/fetch_orders_cli.py --count 1000

# Extract orders from date range
python scripts/fetch_orders_cli.py --start-date 2024-01-01 --end-date 2024-12-31
```

## Testing

### Run Test Suite
```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src

# Run specific test file
pytest tests/test_items_extractor.py

# Run with verbose output
pytest -v
```

### Test Coverage
- **84/84 tests passing**
- **13/14 API tests passing** (1 skipped)
- **98%+ code coverage**

## Troubleshooting

### Common Issues

#### Authentication Errors
```bash
# Check token status
python -c "from src.extractors.ml_api_client import MLClient; print(MLClient().get_user_info())"

# Refresh tokens
python scripts/refresh_tokens.py
```

#### Database Issues
```bash
# Reset database
rm data/noneca_analytics.db
python main.py  # Recreates database
```

#### API Rate Limiting
```python
# Adjust rate limiting in config
RATE_LIMIT_DELAY = 2.0  # Increase delay
MAX_RETRIES = 5         # Increase retries
```

### Debug Mode
```bash
# Enable debug logging
export LOG_LEVEL=DEBUG
python main.py
```

### Performance Optimization
```python
# Batch processing for large datasets
BATCH_SIZE = 100
CONCURRENT_REQUESTS = 5

# Database optimization
BULK_INSERT_SIZE = 1000
```

## Best Practices

### Data Extraction
- **Start Small**: Begin with 500 orders for initial analysis
- **Regular Updates**: Run daily extractions for current data
- **Error Handling**: Always check extraction logs for issues
- **Rate Limiting**: Respect API limits to avoid blocking

### Database Management
- **Regular Backups**: Schedule automated database backups
- **Index Optimization**: Monitor query performance
- **Data Validation**: Verify data integrity after extractions
- **Cleanup**: Remove old temporary files regularly

### Monitoring
- **Log Analysis**: Regular review of extraction logs
- **Performance Metrics**: Track extraction success rates
- **Data Quality**: Validate extracted data completeness
- **API Usage**: Monitor API quota consumption

## Advanced Usage

### Custom Extractors
```python
class CustomItemsExtractor(ItemsExtractor):
    def extract_premium_items(self, min_price=100):
        # Custom extraction logic
        pass
```

### Data Enrichment
```python
from src.transformers.product_enricher import ProductEnricher

enricher = ProductEnricher()
enriched_data = enricher.enrich_product_data(raw_items)
```

### Automated Scheduling
```python
import schedule
import time

def run_daily_extraction():
    # Run both pipelines
    subprocess.run(["python", "main.py"])
    subprocess.run(["python", "scripts/fetch_orders_cli.py", "--count", "100"])

schedule.every().day.at("02:00").do(run_daily_extraction)

while True:
    schedule.run_pending()
    time.sleep(1)
```

## Support

### Documentation
- **ETL Architecture**: `docs/etl_architecture.md`
- **Database Schema**: `docs/database_schema.md`
- **Development Log**: `docs/development_log.md`

### Community
- **Issues**: GitHub Issues for bug reports
- **Discussions**: GitHub Discussions for questions
- **Contributions**: Pull requests welcome

### Performance Benchmarks
- **Product Extraction**: ~76 items/minute
- **Order Extraction**: ~50 orders/minute
- **Database Operations**: ~1000 inserts/second
- **API Success Rate**: 98%+