# Database Schema

## Overview

The platform uses SQLite with SQLAlchemy ORM to store comprehensive marketplace data. The schema supports both product catalog analytics and order transaction intelligence with full relationship mapping.

## Core Tables

### Items Table
Stores product catalog data with business intelligence attributes.

```sql
CREATE TABLE items (
    item_id VARCHAR(50) PRIMARY KEY,
    title VARCHAR(500),
    category_id VARCHAR(50),
    current_price DECIMAL(12,2),
    original_price DECIMAL(12,2),
    available_quantity INTEGER,
    sold_quantity INTEGER,
    condition VARCHAR(20),
    -- Business attributes
    brand VARCHAR(100),
    size VARCHAR(20),
    color VARCHAR(50),
    gender VARCHAR(20),
    -- Performance metrics
    views INTEGER DEFAULT 0,
    conversion_rate DECIMAL(5,4),
    seller_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**Key Features**:
- Product identification and categorization
- Pricing and inventory tracking
- Business attribute extraction
- Performance metrics calculation
- Seller relationship mapping

### Orders Table
Stores order transaction data for sales intelligence.

```sql
CREATE TABLE orders (
    order_id BIGINT PRIMARY KEY,
    seller_id INTEGER,
    buyer_id INTEGER,
    date_created TIMESTAMP,
    date_closed TIMESTAMP,
    last_updated TIMESTAMP,
    status VARCHAR(50),
    status_detail VARCHAR(100),
    -- Financial data
    total_amount DECIMAL(12,2),
    paid_amount DECIMAL(12,2),
    currency_id VARCHAR(10),
    -- Shipping information
    shipping_cost DECIMAL(12,2),
    shipping_mode VARCHAR(50),
    shipping_status VARCHAR(50),
    -- Order context
    order_type VARCHAR(50),
    payment_type VARCHAR(50),
    -- Tracking
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**Key Features**:
- Complete transaction lifecycle tracking
- Financial data with currency support
- Shipping and logistics information
- Order status and payment tracking
- Timezone-aware timestamp handling

### Order Items Table
Links orders to specific products with detailed transaction data.

```sql
CREATE TABLE order_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id BIGINT,
    item_id VARCHAR(50),
    quantity INTEGER,
    unit_price DECIMAL(12,2),
    full_unit_price DECIMAL(12,2),
    currency_id VARCHAR(10),
    -- Performance correlation
    sale_fee DECIMAL(12,2),
    manufacturing_days INTEGER,
    -- Relationships
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (item_id) REFERENCES items(item_id)
);
```

**Key Features**:
- Product-to-order relationship mapping
- Detailed pricing and quantity data
- Sales fee and manufacturing tracking
- Product performance correlation support

## Supporting Tables

### Price History Table
Tracks pricing changes and competitive positioning over time.

```sql
CREATE TABLE price_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    item_id VARCHAR(50),
    price DECIMAL(12,2),
    discount_percentage DECIMAL(5,2),
    competitor_rank INTEGER,
    price_position VARCHAR(20),
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Enhanced Sellers Table
Stores seller information with performance metrics.

```sql
CREATE TABLE sellers (
    seller_id INTEGER PRIMARY KEY,
    nickname VARCHAR(100),
    reputation_score DECIMAL(3,2),
    transactions_completed INTEGER,
    is_competitor BOOLEAN DEFAULT FALSE,
    market_share_pct DECIMAL(5,2),
    -- Sales performance metrics
    total_orders INTEGER DEFAULT 0,
    total_revenue DECIMAL(15,2) DEFAULT 0,
    avg_order_value DECIMAL(12,2) DEFAULT 0,
    last_order_date TIMESTAMP
);
```

### Sales Analytics Table
Aggregated sales metrics for business intelligence.

```sql
CREATE TABLE sales_analytics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    seller_id INTEGER,
    analysis_date DATE,
    period_type VARCHAR(20), -- 'daily', 'weekly', 'monthly'
    -- Volume metrics
    orders_count INTEGER,
    items_sold INTEGER,
    total_revenue DECIMAL(15,2),
    avg_order_value DECIMAL(12,2),
    -- Performance metrics
    conversion_rate DECIMAL(5,4),
    return_rate DECIMAL(5,4),
    customer_acquisition_cost DECIMAL(12,2),
    -- Trends
    revenue_growth_rate DECIMAL(5,4),
    order_frequency_trend DECIMAL(5,4)
);
```

## Relationships

### Entity Relationship Diagram

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│    Items    │────►│    Sellers  │◄────│   Orders    │
│             │     │             │     │             │
│ item_id (PK)│     │seller_id(PK)│     │order_id (PK)│
│ seller_id   │     │ nickname    │     │ seller_id   │
│ title       │     │ reputation  │     │ total_amount│
│ price       │     │ market_share│     │ status      │
└─────────────┘     └─────────────┘     └─────────────┘
       │                                        │
       │            ┌─────────────┐            │
       └───────────►│Order Items  │◄───────────┘
                    │             │
                    │ id (PK)     │
                    │ order_id    │
                    │ item_id     │
                    │ quantity    │
                    │ unit_price  │
                    └─────────────┘
```

### Key Relationships
- **Items ↔ Sellers**: Many-to-one (multiple products per seller)
- **Orders ↔ Sellers**: Many-to-one (multiple orders per seller)
- **Items ↔ Orders**: Many-to-many via `order_items` (products in multiple orders)
- **Items ↔ Price History**: One-to-many (price tracking over time)

## Data Types and Constraints

### Precision Requirements
- **Currency**: `DECIMAL(12,2)` for monetary values
- **Percentages**: `DECIMAL(5,4)` for rates and percentages
- **Large Numbers**: `BIGINT` for order IDs and transaction volumes
- **Timestamps**: Timezone-aware datetime handling

### Indexing Strategy
```sql
-- Performance optimization indexes
CREATE INDEX idx_items_seller_id ON items(seller_id);
CREATE INDEX idx_items_category ON items(category_id);
CREATE INDEX idx_orders_seller_id ON orders(seller_id);
CREATE INDEX idx_orders_date_created ON orders(date_created);
CREATE INDEX idx_order_items_order_id ON order_items(order_id);
CREATE INDEX idx_order_items_item_id ON order_items(item_id);
CREATE INDEX idx_price_history_item_id ON price_history(item_id);
```

## Data Validation

### Constraints and Rules
- **Primary Keys**: Auto-generated or ML-provided unique identifiers
- **Foreign Keys**: Referential integrity enforcement
- **Null Handling**: Required fields marked as NOT NULL
- **Data Types**: Strict type enforcement with validation
- **Timestamps**: Automatic creation and update tracking

### Business Rules
- Prices must be positive values
- Quantities cannot be negative
- Conversion rates bounded between 0 and 1
- Timestamps must be timezone-aware (São Paulo)
- Currency codes follow ISO 4217 standards

## Migration Strategy

### Version Control
- SQLAlchemy Alembic for schema migrations
- Backward compatibility maintenance
- Data integrity verification during migrations
- Rollback capabilities for critical changes

### Future Schema Enhancements
- Additional product attributes for ML algorithms
- Customer behavior tracking tables
- Advanced analytics aggregation tables
- Real-time data streaming support tables