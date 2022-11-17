SELECT

  DATE_TRUNC('MONTH', order_created_at_ts) AS month,
  COALESCE(brand_name, organization_name) AS brand_name,
  CASE WHEN brand_licensee_type = 'FUTURE_FOODS' THEN TRUE ELSE FALSE END AS is_ff_brand,
  facility_id,
  facility_latitude  AS latitude,
  facility_longitude AS longitude,
  facility_timezone AS timezone,
  facility_country_code AS country_code,
  store_id,
  item_id,
  TRIM(item_name) AS item_name,
  TRIM(normalized_item_name) AS normalized_item_name,
  currency_code,
  COUNT(DISTINCT day_partition) AS number_of_days,
  ROUND(SUM(quantity), 2) AS total_qty_ordered,
  COUNT(DISTINCT order_id) AS total_orders,
  ROUND(SUM(quantity) / COUNT(DISTINCT day_partition), 2) AS quantity_per_day,
  ROUND(SUM(quantity) / COUNT(DISTINCT order_id), 2) AS quantity_per_order,
  ROUND(SUM(total) / SUM(quantity), 2) AS price
  
FROM hudi_ingest.analytics_views.customer_order_items 

WHERE 1=1
  AND day_partition BETWEEN CAST('{begin_date}' AS VARCHAR) AND CAST('{end_date}' AS VARCHAR)
  AND is_cancelled = FALSE
  AND facility_country_code = '{country_code}'
  AND is_parent = TRUE
  AND normalized_item_name IS NOT NULL
  AND total IS NOT NULL
  AND quantity IS NOT NULL
  
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13

