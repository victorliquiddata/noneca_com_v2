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
