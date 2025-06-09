SELECT sql 
FROM sqlite_master 
WHERE type IN ('table', 'index', 'trigger', 'view') 
  AND name NOT LIKE 'sqlite_%';


  
  
  
  
  
  
  
  
WITH table_data AS (
  -- sellers table
  SELECT 'sellers' as table_name, seller_id as pk, 
         ROW_NUMBER() OVER (ORDER BY seller_id) as rn,
         COUNT(*) OVER () as total_rows,
         seller_id || '|' || COALESCE(nickname, '') || '|' || COALESCE(reputation_score, '') || '|' || 
         COALESCE(transactions_completed, '') || '|' || COALESCE(is_competitor, '') || '|' || 
         COALESCE(market_share_pct, '') as row_data
  FROM sellers
  
  UNION ALL
  
  -- market_trends table  
  SELECT 'market_trends' as table_name, id as pk,
         ROW_NUMBER() OVER (ORDER BY id) as rn,
         COUNT(*) OVER () as total_rows,
         id || '|' || COALESCE(keyword, '') || '|' || COALESCE(search_volume, '') || '|' || 
         COALESCE(category_id, '') || '|' || COALESCE(trend_date, '') || '|' || 
         COALESCE(growth_rate, '') as row_data
  FROM market_trends
  
  UNION ALL
  
  -- buyers table
  SELECT 'buyers' as table_name, buyer_id as pk,
         ROW_NUMBER() OVER (ORDER BY buyer_id) as rn,
         COUNT(*) OVER () as total_rows,
         buyer_id || '|' || COALESCE(nickname, '') as row_data
  FROM buyers
  
  UNION ALL
  
  -- items table
  SELECT 'items' as table_name, CAST(item_id AS INTEGER) as pk,
         ROW_NUMBER() OVER (ORDER BY item_id) as rn,
         COUNT(*) OVER () as total_rows,
         item_id || '|' || COALESCE(title, '') || '|' || COALESCE(category_id, '') || '|' || 
         COALESCE(current_price, '') || '|' || COALESCE(original_price, '') || '|' || 
         COALESCE(available_quantity, '') || '|' || COALESCE(sold_quantity, '') || '|' || 
         COALESCE(condition, '') || '|' || COALESCE(brand, '') || '|' || COALESCE(size, '') || '|' || 
         COALESCE(color, '') || '|' || COALESCE(gender, '') || '|' || COALESCE(views, '') || '|' || 
         COALESCE(conversion_rate, '') || '|' || COALESCE(seller_id, '') || '|' || 
         COALESCE(created_at, '') || '|' || COALESCE(updated_at, '') as row_data
  FROM items
  
  UNION ALL
  
  -- orders table
  SELECT 'orders' as table_name, order_id as pk,
         ROW_NUMBER() OVER (ORDER BY order_id) as rn,
         COUNT(*) OVER () as total_rows,
         order_id || '|' || COALESCE(status, '') || '|' || COALESCE(total_amount, '') || '|' || 
         COALESCE(total_fees, '') || '|' || COALESCE(profit_margin, '') || '|' || 
         COALESCE(currency_id, '') || '|' || COALESCE(date_created, '') || '|' || 
         COALESCE(date_closed, '') || '|' || COALESCE(seller_id, '') || '|' || 
         COALESCE(buyer_id, '') as row_data
  FROM orders
  
  UNION ALL
  
  -- price_history table
  SELECT 'price_history' as table_name, id as pk,
         ROW_NUMBER() OVER (ORDER BY id) as rn,
         COUNT(*) OVER () as total_rows,
         id || '|' || COALESCE(item_id, '') || '|' || COALESCE(price, '') || '|' || 
         COALESCE(discount_percentage, '') || '|' || COALESCE(competitor_rank, '') || '|' || 
         COALESCE(price_position, '') || '|' || COALESCE(recorded_at, '') as row_data
  FROM price_history
  
  UNION ALL
  
  -- order_items table
  SELECT 'order_items' as table_name, order_item_id as pk,
         ROW_NUMBER() OVER (ORDER BY order_item_id) as rn,
         COUNT(*) OVER () as total_rows,
         order_item_id || '|' || COALESCE(quantity, '') || '|' || COALESCE(unit_price, '') || '|' || 
         COALESCE(sale_fee, '') || '|' || COALESCE(listing_type, '') || '|' || 
         COALESCE(variation_id, '') || '|' || COALESCE(order_id, '') || '|' || 
         COALESCE(item_id, '') as row_data
  FROM order_items
)
SELECT 
  table_name,
  CASE 
    WHEN rn <= 5 THEN 'HEADER'
    WHEN rn > (total_rows - 5) THEN 'FOOTER'
  END as section,
  rn as row_number,
  total_rows,
  row_data
FROM table_data
WHERE rn <= 5 OR rn > (total_rows - 5)
ORDER BY table_name, rn;