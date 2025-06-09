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
